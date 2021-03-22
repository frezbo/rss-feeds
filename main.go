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
	channelID  = "cwt9qwjzb7gjzca5d8u5s49ewo"
	feedFile   = "feeds.txt"
	colorRed   = "\033[31m"
	colorGreen = "\033[32m"
	colorReset = "\033[0m"
)

// Subscriptions contain all the subscriptions data
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
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer db.Close()
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
	localSubscriptions, err := generateFeedFromFile(feedFile, channelID)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	updatedSubscriptions := generateFeedForRemote(localSubscriptions, remoteSubscriptions)
	if len(updatedSubscriptions.Subscriptions) == len(remoteSubscriptions.Subscriptions) {
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

func generateFeedForRemote(local, remote *Subscriptions) *Subscriptions {
	updatedRemote := &Subscriptions{Subscriptions: map[string]*Subscription{}}
	var newSubsToAdd = make(map[string]*Subscription, len(local.Subscriptions))

	// do a deep clone of the existing remote
	for subscriptionID, subscriptionData := range remote.Subscriptions {
		updatedRemote.Subscriptions[subscriptionID] = subscriptionData
	}

	for subscriptionID, subscriptionData := range local.Subscriptions {
		newSubsToAdd[subscriptionID] = subscriptionData
	}

	for i, j := range remote.Subscriptions {
		switch val, ok := local.Subscriptions[i]; {
		case ok && val.URL == j.URL:
			delete(newSubsToAdd, i)
		case !ok:
			delete(updatedRemote.Subscriptions, i)
			fmt.Printf("%s-\t%s\n", string(colorRed), j.URL)
		}
	}
	for i, j := range newSubsToAdd {
		fmt.Printf("%s+\t%s\n", string(colorGreen), j.URL)
		updatedRemote.Subscriptions[i] = j
	}
	fmt.Print(string(colorReset))
	return updatedRemote
}

func generateFeedFromFile(filename, channelID string) (*Subscriptions, error) {
	src, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	split := strings.Split(string(src), "\n")
	subs := &Subscriptions{Subscriptions: map[string]*Subscription{}}
	for _, url := range split {
		// skip commented out feeds
		if strings.HasPrefix(url, "# ") {
			continue
		}
		// skip empty lines
		if url == "" {
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
		if strings.Contains(err.Error(), "duplicate key value violates unique constraint") {
			result, err = db.Exec(`update pluginkeyvaluestore set pvalue=$1 where pluginid='rssfeed' and pkey='subscriptions'`, bytes)
			if err != nil {
				return 0, err
			}
		}

		return 0, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(rowsAffected), nil
}
