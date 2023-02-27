from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth) 

gfile = drive.CreateFile()
# Read file and set it as the content of this instance.
gfile.SetContentFile('test.csv')
gfile.Upload() # Upload the file.

