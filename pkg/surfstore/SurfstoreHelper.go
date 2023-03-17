package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) values(`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	// create the file
	indexFile, _ := os.Create(outputMetaPath)
	defer indexFile.Close()
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	// 1. read all the fileMetaData and write them back to the database file
	for _, value := range fileMetas {
		curStatement := "" + insertTuple
		curStatement += ("'" + value.Filename + "', " + strconv.Itoa(int(value.Version)))
		for curIndex, curStr := range value.BlockHashList {
			curHashIndex := ", " + strconv.Itoa(curIndex)
			curHash := ", '" + curStr + "')"
			finalStatement := curStatement + curHashIndex + curHash
			//fmt.Println(finalStatement)
			statement, err := db.Prepare(finalStatement)
			if err != nil {
				log.Fatal("Error During database insert")
			}
			statement.Exec()
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = ``

const getTuplesByFileName string = ``

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	// 1. load the whole database 2. construct newe struct FileMetaMap
	// 3. fill in the filemetamap using the rows: 4. add to fileMetaMap
	rows, err := db.Query("SELECT * FROM indexes ORDER BY fileName, hashIndex ASC")
	if err != nil {
		log.Fatal("load database error")
		return nil, err
	}
	for rows.Next() {
		var dbFileName string
		var dbFileVersion int32
		var dbHashIndex int
		var dbHash string
		err = rows.Scan(&dbFileName, &dbFileVersion, &dbHashIndex, &dbHash)
		if err != nil {
			log.Fatal("scan row error")
		}
		//log.Println(dbFileName, ": ", dbHashIndex)
		// if empty: construct a new filemata
		if _, ok := fileMetaMap[dbFileName]; !ok {
			curFileMetaData := &FileMetaData{}
			curFileMetaData.Filename = dbFileName
			curFileMetaData.Version = dbFileVersion
			curFileMetaData.BlockHashList = append(curFileMetaData.BlockHashList, dbHash)
			fileMetaMap[dbFileName] = curFileMetaData
		} else {
			fileMetaMap[dbFileName].BlockHashList = append(fileMetaMap[dbFileName].BlockHashList, dbHash)
		}

	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version, len(filemeta.BlockHashList))
		/*for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}*/

	}

	fmt.Println("---------END PRINT MAP--------")

}
