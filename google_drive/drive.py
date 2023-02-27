from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

def upload_to_googledrive(filename):
    gauth = GoogleAuth()           
    drive = GoogleDrive(gauth) 

    gfile = drive.CreateFile()
    # Read file and set it as the content of this instance.
    gfile.SetContentFile(filename)
    gfile.Upload() # Upload the file.
