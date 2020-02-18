package notbadger

import (
	"github.com/elliotcourant/notbadger/table"
	"github.com/elliotcourant/notbadger/z"
	"io/ioutil"
)

func getFileIdMap(directory string) (idMap map[PartitionId]map[uint64]struct{}) {
	fileInfoList, err := ioutil.ReadDir(directory)
	z.Check(err)

	idMap = map[PartitionId]map[uint64]struct{}{}
	for _, info := range fileInfoList {
		if info.IsDir() {
			continue
		}

		partitionId, fileId, ok := table.ParseFileId(info.Name())
		if !ok {
			continue
		}

		if _, ok := idMap[PartitionId(partitionId)]; !ok {
			idMap[PartitionId(partitionId)] = map[uint64]struct{}{}
		}

		idMap[PartitionId(partitionId)][fileId] = struct{}{}
	}

	return idMap
}

