package main

import (
	"context"
	"github.com/bluemediaapp/models"
	"github.com/gofiber/fiber/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"math"
	"os"
	"strconv"
)

var (
	app    = fiber.New()
	client *mongo.Client
	config *Config

	mctx = context.Background()

	videosCollection *mongo.Collection
	likedVideosCollection *mongo.Collection
	usersCollection *mongo.Collection
	watchedVideosCollection *mongo.Collection

)

func main() {
	config = &Config{
		port:     os.Getenv("port"),
		mongoUri: os.Getenv("mongo_uri"),
	}

	app.Post("/like/:video_id/:user_id", func(ctx *fiber.Ctx) error {
		userId, err := strconv.ParseInt(ctx.Params("user_id"), 10, 64)
		if err != nil {
			return err
		}
		videoId, err := strconv.ParseInt(ctx.Params("video_id"), 10, 64)
		if err != nil {
			return err
		}

		if hasLiked(userId, videoId) {
			_ = ctx.SendStatus(412)
			_ = ctx.SendString("User has already liked this post")
			return nil
		}

		user, err := getUser(userId)
		if err != nil {
			return err
		}

		video, err := getVideo(videoId)
		if err != nil {
			return err
		}

		err = likeVideo(user, video)


		return err
	})
	app.Post("/watch/:video_id/:user_id", func(ctx *fiber.Ctx) error {
		userId, err := strconv.ParseInt(ctx.Params("user_id"), 10, 64)
		if err != nil {
			return err
		}
		videoId, err := strconv.ParseInt(ctx.Params("video_id"), 10, 64)
		if err != nil {
			return err
		}

		if hasWatched(userId, videoId) {
			_ = ctx.SendStatus(412)
			_ = ctx.SendString("User has already watched this post")
			return nil
		}

		user, err := getUser(userId)
		if err != nil {
			return err
		}

		video, err := getVideo(videoId)
		if err != nil {
			return err
		}

		err = watchVideo(user, video)
		if err != nil {
			return err
		}
		return nil
	})

	initDb()
	log.Fatal(app.Listen(config.port))
}

func initDb() {
	// Connect mongo
	var err error
	client, err = mongo.NewClient(options.Client().ApplyURI(config.mongoUri))
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(mctx)
	if err != nil {
		log.Fatal(err)
	}

	// Setup tables
	db := client.Database("blue")
	videosCollection = db.Collection("video_metadata")
	likedVideosCollection = db.Collection("liked_videos")
	watchedVideosCollection = db.Collection("watched_videos")
	usersCollection = db.Collection("users")
}

// Liking
func hasLiked(userId int64, videoId int64) bool {
	filter := bson.D{{"user_id", userId}, {"video_id", videoId}}
	var limit int64 = 1
	documentCount, err := likedVideosCollection.CountDocuments(mctx, filter, &options.CountOptions{
		Limit: &limit,
	})
	if err != nil {
		log.Print(err)
		return true
	}
	return documentCount == int64(1)
}
func likeVideo(user models.DatabaseUser, video models.DatabaseVideo) error {
	// Duplicate checks
	likeEvent := models.DatabaseLikeEvent{
		VideoId: video.Id,
		UserId:  user.Id,
	}
	_, err := likedVideosCollection.InsertOne(mctx, likeEvent)
	if err != nil {
		return err
	}

	// Interests
	interests := make(map[string]int64)
	for _, tag := range video.Tags {
		currentInterestValue, exists := user.Interests[tag]
		if !exists {
			currentInterestValue = 0
		}
		currentInterestValue += 11
		interests[tag] = currentInterestValue
	}
	modifyInterests(user, interests)


	// Like count
	if video.Likes >= math.MaxInt64 - 1 {
		log.Printf("Max likes on video %d", video.Id)
		return err
	}
	_, err = videosCollection.UpdateOne(mctx, bson.D{{"_id", video.Id}}, bson.D{{"$inc", bson.D{{"likes", 1}}}})
	if err != nil {
		return err
	}

	return nil
}

// Watching
func watchVideo(user models.DatabaseUser, video models.DatabaseVideo) error {
	watchEvent := models.DatabaseWatchEvent{
		VideoId: video.Id,
		UserId:  user.Id,
	}
	_, err := watchedVideosCollection.InsertOne(mctx, watchEvent)
	if err != nil {
		return err
	}
	interests := make(map[string]int64)
	for _, tag := range video.Tags {
		currentInterestValue, exists := user.Interests[tag]
		if !exists {
			currentInterestValue = 0
		}
		currentInterestValue -= 1
		interests[tag] = currentInterestValue
	}
	modifyInterests(user, interests)

	return nil
}

func hasWatched(userId int64, videoId int64) bool {
	filter := bson.D{{"user_id", userId}, {"video_id", videoId}}
	var limit int64 = 1
	documentCount, err := watchedVideosCollection.CountDocuments(mctx, filter, &options.CountOptions{
		Limit: &limit,
	})
	if err != nil {
		log.Print(err)
		return true
	}
	return documentCount == int64(1)
}


// Db utils
func getUser(userId int64) (models.DatabaseUser, error) {
	query := bson.D{{"_id", userId}}
	rawUser := usersCollection.FindOne(mctx, query)
	var user models.DatabaseUser
	err := rawUser.Decode(&user)
	if err != nil {
		return models.DatabaseUser{}, err
	}
	return user, nil
}

func getVideo(videoId int64) (models.DatabaseVideo, error) {
	query := bson.D{{"_id", videoId}}
	rawVideo := videosCollection.FindOne(mctx, query)
	var video models.DatabaseVideo
	err := rawVideo.Decode(&video)
	if err != nil {
		return models.DatabaseVideo{}, err
	}
	return video, nil
}
func modifyInterests(user models.DatabaseUser, interests map[string]int64)  {
	// Interests
	for name, value := range interests {
		currentInterestValue, exists := user.Interests[name]
		if !exists {
			currentInterestValue = 0
		}
		currentInterestValue += value
		user.Interests[name] = currentInterestValue
	}
	update := bson.D{{"$set", bson.D{{"interests", user.Interests}}}}
	filter := bson.D{{"_id", user.Id}}

	_, err := usersCollection.UpdateOne(mctx, filter, update)
	if err != nil {
		log.Print(err)
		return
	}
}
