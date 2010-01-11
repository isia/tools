from optparse import OptionParser
import os
import shutil
import tarfile
import time
import sqlite3

def main():
    usage = "usage: %prog [options] rmapsDir mapTarFile"
    parser = OptionParser(usage)
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose")
    
    (options, args) = parser.parse_args()
    if len(args) != 2:
        parser.error("incorrect number of arguments")
    

    (rmapsDir, mapTarFile) = args

    tmpFolder = "/tmp/rmaps_index_upader/";

    indexDbFile = rmapsDir + '/data/index.db'
    indexDbFileTmp = tmpFolder + "/index.db"

    (mapTarPath, mapTarName) = os.path.split(mapTarFile)
    mapTarFileTmp = tmpFolder + mapTarName

    zooms = []

    if not os.path.exists(indexDbFile):
        print "Index file not found. Exiting..."
        return
    
    if not options.quiet :
        print "Copying files to /tmp folder..."
        
    if not os.path.exists(tmpFolder):
        os.makedirs(tmpFolder)
    shutil.copyfile(indexDbFile, indexDbFileTmp)
    shutil.copyfile(mapTarFile, mapTarFileTmp)

    dbTableName = "cahs_" + mapTarName.replace('.', '_'); #cahs!!! idk if it will work with correct name

    if not options.quiet :
        print "Opening db..."
    
    con = sqlite3.connect(indexDbFileTmp)
    cur = con.cursor()
    

    cur.execute("DROP TABLE '"+dbTableName+"';")
    con.commit()
    cur.execute("CREATE TABLE '"+dbTableName+"' (name VARCHAR(100), offset INTEGER NOT NULL, size INTEGER NOT NULL, PRIMARY KEY(name) );")
    con.commit()

    if not options.quiet :
        print "Parsing tar file and filling db..."

    tar = tarfile.open(mapTarFileTmp, "r")
    mapTarFileSize = os.path.getsize(mapTarFileTmp);
    for tarinfo in tar:
        
        
        if tarinfo.isreg():
            cur.execute("INSERT INTO '" + dbTableName + "' VALUES ('" + tarinfo.name + "', " + str(tarinfo.offset_data) + ", " + str(tarinfo.size) + ");")

        elif tarinfo.isdir():
            zooms.append(int(tarinfo.name[:-1]))

        else:
            print "unknow file founded."
    tar.close()
    
    minZoom = min(zooms)
    maxZoom = max(zooms)

    cur.execute("INSERT OR REPLACE INTO ListCashTables VALUES('" + dbTableName + "',"+ str(time.time()).replace(".","") +"0,"+ str(mapTarFileSize) +", "+str(minZoom)+","+str(maxZoom)+")");

    if not options.quiet :
        print "Commiting..."

    con.commit()

    if not options.quiet :
        print "Uploading index back to rmaps dir..."

    indexDbFileBkup = indexDbFile + "_bkup_" + time.strftime("%y%m%d%H%M%S")
    shutil.copyfile(indexDbFile, indexDbFileBkup)
    shutil.copyfile(indexDbFileTmp, indexDbFile)

    if not options.quiet :
        print "Done."
        print "You have a backup file ", indexDbFile + "_bkup_" + time.strftime("%y%m%d%H%M%S") , " in case of problems you can restore it."
        print "Thank you, have a nice day!"


def dump(obj):
    for attr in dir(obj):
        print "obj.%s = %s" % (attr, getattr(obj, attr))

if __name__ == "__main__":
    main()

