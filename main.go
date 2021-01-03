package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

const (
	host   = "localhost"
	port   = 5432
	user   = "mattermost"
	dbname = "mattermost"
	// mattermost channel id
	channelID = "cwt9qwjzb7gjzca5d8u5s49ewo"
	feedFile  = "feeds.txt"
)

// Subscriptions contain all teh subscriptions data
type Subscriptions struct {
	Subscriptions map[string]*Subscription
}

// Subscription stores the feed info
type Subscription struct {
	ChannelID string
	URL       string
	XMLInfo   *XMLFeedMeta
}

// XMLFeedMeta stores metadata about the feed
type XMLFeedMeta struct {
	ID, Title, GUID, PubDate string `json:",omitempty"`
}

func main() {
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		fmt.Println("POSTGRES_PASSWORD needs to be set")
		os.Exit(1)
	}
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	defer db.Close()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = db.Ping()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	remoteSubscriptions, err := readFeedFromDB(db)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	localSubscriptions, err := generateFeedFromFIle(feedFile, channelID)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	updatedSubscriptions := generateFeedForDB(localSubscriptions, remoteSubscriptions)
	if updatedSubscriptions == nil {
		fmt.Println("remote in sync with feed list")
	} else {
		bytes, err := json.Marshal(updatedSubscriptions)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		rowsAffected, err := writeFeedToDB(db, bytes)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Printf("%d rows affected\n", rowsAffected)
	}
}

func generateFeedForDB(local, remote *Subscriptions) *Subscriptions {
	var count int
	for subscriptionID, SubscriptionData := range local.Subscriptions {
		if _, ok := remote.Subscriptions[subscriptionID]; !ok {
			count++
			remote.Subscriptions[subscriptionID] = SubscriptionData
		}
	}
	// if count is zero, there is no change
	if count == 0 {
		return nil
	}
	return remote
}

func generateFeedFromFIle(filename, channelID string) (*Subscriptions, error) {
	src, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	split := strings.Split(string(src), "\n")
	split = split[0 : len(split)-1]
	subs := &Subscriptions{Subscriptions: map[string]*Subscription{}}
	for _, url := range split {
		// skip commented out feeds
		if strings.HasPrefix(url, "# ") {
			continue
		}
		subscriptionID := fmt.Sprintf("%s/%s", channelID, url)
		subs.Subscriptions[subscriptionID] = &Subscription{
			ChannelID: channelID,
			URL:       url,
		}
	}
	return subs, nil
}

func readFeedFromDB(db *sql.DB) (*Subscriptions, error) {
	rows, err := db.Query(`select pvalue from pluginkeyvaluestore where pluginid='rssfeed' and pkey='subscriptions'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var scanBytes []byte
	for rows.Next() {
		err = rows.Scan(&scanBytes)
		if err != nil {
			return nil, err
		}
	}
	var decode *Subscriptions
	err = json.Unmarshal(scanBytes, &decode)
	if err != nil {
		return nil, err
	}
	return decode, nil
}

func writeFeedToDB(db *sql.DB, bytes []byte) (int, error) {
	result, err := db.Exec(`insert into pluginkeyvaluestore(pluginid, pkey, pvalue, expireat) values('rssfeed', 'subscriptions', $1, 0)`, bytes)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(rowsAffected), nil
}
