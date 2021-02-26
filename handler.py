from minio import Minio
import requests
import os
import numpy as np
import json
import psycopg2
import gzip
from astropy.io import fits
from skimage import exposure
from skimage import io
import uuid


def gunzip(source_filepath, dest_filepath, block_size=65536):
    with gzip.open(source_filepath, 'rb') as s_file, \
            open(dest_filepath, 'wb') as d_file:
        while True:
            block = s_file.read(block_size)
            if not block:
                break
            else:
                d_file.write(block)
        d_file.write(block)


def splitall(path):
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path:  # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts


def handle(req):
    print("Thumbnail generation triggered")
    try:
        # read json
        data = json.loads(req)
        source = data['preview_file']
        id = data['preview_id']

        # minio client
        mc = Minio(os.environ['minio_hostname'],
                   access_key=os.environ['minio_access_key'],
                   secret_key=os.environ['minio_secret_key'],
                   secure=False)

        # get source file
        if (os.path.splitext(source)[1] == '.gz'):
            print("Downloading gz file from minio")
            gzTmpSource = '/tmp/source_' + uuid.uuid4().hex + '_' + source
            mc.fget_object('test', source, gzTmpSource)
            print("File downloaded")
            tmpSource = '/tmp/source_' + uuid.uuid4().hex + '_' + \
                os.path.splitext(source)[0]
            if not os.path.exists(os.path.dirname(tmpSource)):
                os.makedirs(os.path.dirname(tmpSource))
            gunzip(gzTmpSource, tmpSource)
            os.remove(gzTmpSource)
            fSource = open(tmpSource, "rb")
        else:
            print("Downloading file from minio")
            tmpSource = '/tmp/source_' + uuid.uuid4().hex + '_' + source
            mc.fget_object('test', source, tmpSource)
            fSource = open(tmpSource, "rb")
            print("File downloaded")

        # generate thumbnail
        print("Generating thumbnail")
        hdus = fits.open(fSource)
        print(hdus)
        for h in hdus:
            print(type(h))
            if isinstance(h, fits.ImageHDU) or isinstance(h, fits.CompImageHDU):
                hdu = h
                break
            elif isinstance(h, fits.PrimaryHDU) and h.data is not None:
                hdu = h
                break

        img = exposure.equalize_adapthist(hdu.data)
        img_min = np.min(img)
        img_max = np.max(img)
        img_norm = ((img.astype(np.float) - img_min) /
                    (img_max - img_min) * 255).astype(np.uint8)

        thumbnailpath = 'thumbnail/'+os.path.splitext(source)[0]+'.png'

        if not os.path.exists(os.path.dirname(thumbnailpath)):
            os.makedirs(os.path.dirname(thumbnailpath))

        io.imsave(thumbnailpath, np.flip(img_norm, 0))
        print("Thumbnail generated: " + thumbnailpath)

        # upload thumbnail to minio
        print("Uploading thumbnail to minio")
        mc.fput_object(os.getenv('minio_bucket', 'test'),
                       thumbnailpath, thumbnailpath)
        print("Thumbnail uploaded")

        try:
            print("Uploading thumbnail to preview public bucket")
            pathParts = splitall(source)
            publicThumbnailPath = f'{pathParts[0]}/latest.png'
            print(f'Public thumbnail path: {publicThumbnailPath}')
            mc.fput_object(os.getenv(
                'minio_prv_bucket', 'test-preview'), publicThumbnailPath, thumbnailpath)
            print("Thumbnail uploaded")
        except (Exception) as error:
            print("Error while uploading to the public bucket", error)

        os.remove(thumbnailpath)

        # save thumbnail information
        print("Saving thumbnail in the database")
        connection = psycopg2.connect(user=os.environ['database_user'],
                                      password=os.environ['database_password'],
                                      host=os.environ['database_host'],
                                      port="5432",
                                      database=os.environ['database'])
        cursor = connection.cursor()
        sql_update_query = """Update files set thumbnail = %s where id = %s"""
        cursor.execute(sql_update_query, (thumbnailpath, id))
        connection.commit()
        count = cursor.rowcount
        print(count, "Thumbnail saved")

    except (Exception) as error:
        print("Error during thumbnail generation", error)
    finally:
        if (fSource):
            fSource.close()
            os.remove(tmpSource)
        if(connection):
            cursor.close()
            connection.close()
