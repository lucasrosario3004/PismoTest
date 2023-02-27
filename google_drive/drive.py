from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

def upload_to_googledrive(filename, id):
    gauth = GoogleAuth()           
    drive = GoogleDrive(gauth) 

    gfile = drive.CreateFile({'id': id,'parents': [{'id': '1u0j3Ks_qYSh2Dyo7T-g9giL-usiJf9f0'}]})
    # Read file and set it as the content of this instance.
    gfile.SetContentFile(filename)
    gfile.Upload() # Upload the file.
