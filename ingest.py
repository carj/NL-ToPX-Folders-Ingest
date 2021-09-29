import math
import logging
import xml.etree.ElementTree as ET
from pyPreservica import *
import configparser
import os
import zipfile



logger = logging.getLogger(__name__)

LOG_FILENAME = 'ingest.log'
logging.basicConfig(level=logging.INFO, filename=LOG_FILENAME, filemode="a")

consoleHandler = logging.StreamHandler()
logging.getLogger().addHandler(consoleHandler)

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def extract_level(topx_file):
    tree = ET.parse(topx_file)
    aggregatieniveau = tree.find(".//{*}aggregatieniveau")
    identificatiekenmerk = tree.find(".//{*}identificatiekenmerk")
    return aggregatieniveau.text, identificatiekenmerk.text


if __name__ == '__main__':
    entity = EntityAPI()
    logger.info(entity)
    upload = UploadAPI()

    config = configparser.ConfigParser()
    config.read('credentials.properties', encoding='utf-8')
    parent_folder_id = config['credentials']['parent.folder']

    security_tag = config['credentials']['security.tag']

    # check parent folder exists
    if parent_folder_id:
        parent = entity.folder(parent_folder_id)
        parent = parent.reference
    else:
        parent = None
    logger.info(f"Packages will be ingested into {parent}")

    data_folder = config['credentials']['data.folder']
    logger.info(f"Packages will be created from folders in {data_folder}")

    group_size = int(config['credentials']['group.size'])
    bucket = config['credentials']['bucket']

    # list archive folders in data directory
    archief_folders = [f.name for f in os.scandir(data_folder) if f.is_dir()]
    for archief in archief_folders:
        logger.info(f"Found {archief} Archief Folder")
        archief_folder = os.path.join(os.path.join(data_folder, archief))
        archief_topx = os.path.join(archief_folder, f"{archief}.metadata")
        assert os.path.isfile(archief_topx)
        level, identifier = extract_level(archief_topx)
        entities = entity.identifier("identificatiekenmerk", identifier)
        if len(entities) == 0:
            folder = entity.create_folder(archief, archief, security_tag, parent)
            entity.add_identifier(folder, "identificatiekenmerk", identifier)
            logger.info(f"adding metadata {archief_topx} To Folder {archief}")
            with open(archief_topx, 'r', encoding="utf-8") as md:
                folder = entity.add_metadata(folder, "http://www.nationaalarchief.nl/ToPX/v2.3", md)
            archief_parent = folder.reference
        else:
            folder = entities.pop()
            archief_parent = folder.reference
        serie_folders = [f.name for f in os.scandir(archief_folder) if f.is_dir()]
        for serie in serie_folders:
            logger.info(f"Found {serie} Serie Folder")
            serie_folder = os.path.join(os.path.join(archief_folder, serie))
            serie_topx = os.path.join(serie_folder, f"{serie}.metadata")
            assert os.path.isfile(serie_topx)
            level, identifier = extract_level(serie_topx)
            entities = entity.identifier("identificatiekenmerk", identifier)
            if len(entities) == 0:
                folder = entity.create_folder(serie, serie, security_tag, archief_parent)
                entity.add_identifier(folder, "identificatiekenmerk", identifier)
                logger.info(f"adding metadata {serie_topx} To Folder {serie}")
                with open(serie_topx, 'r', encoding="utf-8") as md:
                    folder = entity.add_metadata(folder, "http://www.nationaalarchief.nl/ToPX/v2.3", md)
                serie_parent = folder.reference
            else:
                folder = entities.pop()
                serie_parent = folder.reference
            dossier_folders = [f.name for f in os.scandir(serie_folder) if f.is_dir()]
            for dossier in dossier_folders:
                logger.info(f"Found {dossier} Dossier Folder")
                dossier_folder = os.path.join(os.path.join(serie_folder, dossier))
                dossier_topx = os.path.join(dossier_folder, f"{dossier}.metadata")
                assert os.path.isfile(dossier_topx)
                level, identifier = extract_level(dossier_topx)
                entities = entity.identifier("identificatiekenmerk", identifier)
                if len(entities) == 0:
                    folder = entity.create_folder(dossier, dossier, security_tag, serie_parent)
                    entity.add_identifier(folder, "identificatiekenmerk", identifier)
                    logger.info(f"adding metadata {dossier_topx} To Folder {dossier}")
                    with open(dossier_topx, 'r', encoding="utf-8") as md:
                        folder = entity.add_metadata(folder, "http://www.nationaalarchief.nl/ToPX/v2.3", md)
                    dossier_parent = folder.reference
                else:
                    folder = entities.pop()
                    dossier_parent = folder.reference

                record_folders = [f.path for f in os.scandir(dossier_folder) if f.is_dir()]

                num_folders = len(record_folders)
                logger.info(f"Found {num_folders} folders to ingest in Dossier {dossier}")

                os.chdir(dossier_folder)

                logger.info(f"Batching ingests into groups of {group_size}")

                logger.info(f"This will require about {math.ceil(num_folders / group_size)} submissions")

                # get a group of folders we can ingest together
                for batch in chunks(record_folders, group_size):
                    batches = [os.path.basename(b) for b in batch]
                    # check for already existing folder in Preservica
                    for f in list(batches):
                        result = entity.identifier("code", f)
                        if len(result) > 0:
                            batches.remove(f)
                    if len(batches) == 0:
                        logger.info(f"All folders in batch exist, skipping upload...")
                    if len(batches) > 0:
                        logger.info(batches)
                        zipfile_name = f"{batches[0]}.zip"
                        zf = zipfile.ZipFile(zipfile_name, "w")
                        for root_folder in batches:
                            for dirname, subdirs, files in os.walk(root_folder):
                                zf.write(dirname)
                                for filename in files:
                                    zf.write(os.path.join(dirname, filename))
                        zf.close()

                        package_size_bytes = int(os.path.getsize(zipfile_name))
                        package_size_MB = int(package_size_bytes // int(1024 * 1024))
                        logger.info(f"Created submission with {group_size} folders and total size of {package_size_MB}MB")

                        if package_size_MB > 256:
                            logger.info(f"Uploading to S3 bucket {bucket}")
                            upload.upload_zip_package_to_S3(path_to_zip_package=zipfile_name, folder=dossier_parent,
                                                            bucket_name=bucket,
                                                            callback=UploadProgressCallback(zipfile_name),
                                                            delete_after_upload=True)
                        else:
                            logger.info(f"Uploading to Preservica")
                            upload.upload_zip_package(path_to_zip_package=zipfile_name, folder=dossier_parent,
                                                      callback=UploadProgressCallback(zipfile_name),
                                                      delete_after_upload=True)
                        logger.info(f"")
