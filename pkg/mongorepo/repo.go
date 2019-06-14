// Package mongorepo/repo.go
package mongorepo

import (
	"fmt"
	pb "github.com/kzozulya1/webpage-word-freq-counter-protobuf/protobuf"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
//	logger "app/pkg/loggerutil"
)

//Interface for manage word freq records
type IRepository interface {
	CreateUpdate(*pb.PageWordFrequency) (*pb.PageWordFrequency, bool, error)
	GetAll(*pb.GetRequestFilter) ([]*pb.PageWordFrequency, error)
	Remove(string) (*pb.PageWordFrequency, error)
}

// MongoRepository data -
type MongoRepository struct {
	Collection *mgo.Collection
}

// MongoRepository methods -

// Create -
func (repository *MongoRepository) CreateUpdate(pageWordFrequency *pb.PageWordFrequency) (*pb.PageWordFrequency, bool, error) {
	//First check if document with specified URL is present
	created := false
	filter := bson.M{"pageurl": pageWordFrequency.GetPageUrl()}
	cnt, err := repository.Collection.Find(filter).Count()
	if err != nil {
		return pageWordFrequency, created, err
	}
	
	//logger.Log(pageWordFrequency,"added_data.log")
	
	if cnt == 0 {
		//Create new record -
		if err := repository.Collection.Insert(pageWordFrequency); err != nil {
			return pageWordFrequency, created, err
		}
		created = true
	} else {
		//Update existing record
		if err = repository.Collection.Update(filter, pageWordFrequency); err != nil {
			return pageWordFrequency, created, err
		}
	}
	return pageWordFrequency, created, nil
}

// GetAll -
func (repository *MongoRepository) GetAll(req *pb.GetRequestFilter) ([]*pb.PageWordFrequency, error) {
	//Fetch filter -
	filter := bson.M{}
	if req.GetPageUrl() != "" {
		//Add PageURL filter -
		filter["pageurl"] = bson.M{"$regex": bson.RegEx{`^.*` + req.GetPageUrl() + `.*$`, "i"}}
	}
	if req.GetWord() != "" {
		//Add Word filter -
		filter["words.value"] = bson.M{"$regex": bson.RegEx{`^.*` + req.GetWord() + `.*$`, "i"}}
	}

	var pageWordFreqs []*pb.PageWordFrequency
	err := repository.Collection.Find(filter).All(&pageWordFreqs)
	return pageWordFreqs, err
}

// Remove -
func (repository *MongoRepository) Remove(pageURL string) (*pb.PageWordFrequency, error) {
	var request = pb.GetRequestFilter{PageUrl: pageURL}
	foundDocuments, err := repository.GetAll(&request)
	if err != nil {
		log.Println("Error get all documents:")
		return nil, err
	}
	//log.Printf("%#v",foundDocuments)
	if len(foundDocuments) != 1 {
		return nil, fmt.Errorf("Document with page URL %s not found", pageURL)
	}
	err = repository.Collection.Remove(bson.M{"pageurl": pageURL})
	if err != nil {
		return nil, err
	}
	return foundDocuments[0], err
}
