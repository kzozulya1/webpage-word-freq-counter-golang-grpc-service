//main.go
package main

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"gopkg.in/mgo.v2"
	"log"
	"net"
	"os"
	// Import the generated protobuf code
	repo "app/pkg/mongorepo"
	pb "github.com/kzozulya1/webpage-word-freq-counter-protobuf/protobuf"
	logger "app/pkg/loggerutil"
)

const (
	MONGO_DB         = "seo_analytics"
	MONGO_COLLECTION = "pagewordfrequency"
)

// Service  implement all of the methods to satisfy the service
// we defined in our protobuf definition.
type service struct {
	repository repo.IRepository
}

// Update records, or create if it doesn't exist
func (s *service) UpdateOrCreatePageWordFrequency(ctx context.Context, req *pb.PageWordFrequency) (*pb.Response, error) {
	document, created, err := s.repository.CreateUpdate(req)
	if err != nil {
		logger.Log(err.Error(),"error.log")
		return &pb.Response{}, err
	}
	if created {
		return &pb.Response{Created: true, PageWordFreq: document}, nil
	} else {
		return &pb.Response{Updated: true, PageWordFreq: document}, nil
	}
}

//Get all word freq records, appy filter pb.GetRequestFilter.PageUrl / pb.GetRequestFilter.Word
func (s *service) GetPageWordFrequency(ctx context.Context, req *pb.GetRequestFilter) (*pb.Response, error) {
	allRecords, err := s.repository.GetAll(req)
	if err != nil {
		logger.Log(err.Error(),"error.log")
		return &pb.Response{}, err
	}
	return &pb.Response{PageWordFreqs: allRecords}, nil
}

//Remove record by pb.GetRequestFilter.PageUrl
func (s *service) RemovePageWordFrequency(ctx context.Context, req *pb.GetRequestFilter) (*pb.Response, error) {
	pageWordFreq, err := s.repository.Remove(req.GetPageUrl())
	response := &pb.Response{Removed: true, PageWordFreq: pageWordFreq}
	if err != nil {
		logger.Log(err.Error(),"error.log")
		response.Removed = false
	}
	return response, err
}

//Main routine -
func main() {
	//Prepare Mongo db connection
	mongoDbURI := os.Getenv("DB_HOST")

	//Create conn session
	session, err := mgo.Dial(mongoDbURI)
	if err != nil {
		logger.Log(err.Error(),"error.log")
		panic(err)
	}
	defer session.Close()

	session.SetMode(mgo.Monotonic, true)
	

	//Setup collection for mongo repo obj
	collection := session.DB(MONGO_DB).C(MONGO_COLLECTION)
	repository := &repo.MongoRepository{collection}

	//Listen gRPC Server
	port := os.Getenv("SERVICE_PORT")
	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Log(err.Error(),"error.log")
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()

	// Register our service with the gRPC server
	pb.RegisterWordFrequencyServiceServer(s, &service{repository})

	// Register reflection service on gRPC server.
	reflection.Register(s)

	log.Println("Running on port:", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
