package wdclient

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/journeymidnight/seaweedfs/weed/glog"
)

type Location struct {
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
}

type vidMap struct {
	sync.RWMutex
	vid2Locations map[uint32][]Location
	r             *rand.Rand
}

func newVidMap() vidMap {
	return vidMap{
		vid2Locations: make(map[uint32][]Location),
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (vc *vidMap) LookupVolumeServerUrl(vid string) (serverUrl string, err error) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return "", err
	}

	return vc.GetRandomLocation(uint32(id))
}

func (vc *vidMap) LookupFileId(fileId string) (fullUrl string, err error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return "", errors.New("Invalid fileId " + fileId)
	}
	serverUrl, lookupError := vc.LookupVolumeServerUrl(parts[0])
	if lookupError != nil {
		return "", lookupError
	}
	return "http://" + serverUrl + "/" + fileId, nil
}

func (vc *vidMap) LookupVolumeServer(fileId string) (volumeServer string, err error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return "", errors.New("Invalid fileId " + fileId)
	}
	serverUrl, lookupError := vc.LookupVolumeServerUrl(parts[0])
	if lookupError != nil {
		return "", lookupError
	}
	return serverUrl, nil
}

func (vc *vidMap) GetVidLocations(vid string) (locations []Location) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return nil
	}
	return vc.GetLocations(uint32(id))
}

func (vc *vidMap) GetLocations(vid uint32) (locations []Location) {
	vc.RLock()
	defer vc.RUnlock()

	return vc.vid2Locations[vid]
}

func (vc *vidMap) GetRandomLocation(vid uint32) (serverUrl string, err error) {
	vc.RLock()
	defer vc.RUnlock()

	locations := vc.vid2Locations[vid]
	if len(locations) == 0 {
		return "", fmt.Errorf("volume %d not found", vid)
	}

	return locations[vc.r.Intn(len(locations))].Url, nil
}

func (vc *vidMap) addLocation(vid uint32, location Location) {
	vc.Lock()
	defer vc.Unlock()

	locations, found := vc.vid2Locations[vid]
	if !found {
		vc.vid2Locations[vid] = []Location{location}
		return
	}

	for _, loc := range locations {
		if loc.Url == location.Url {
			return
		}
	}

	vc.vid2Locations[vid] = append(locations, location)

}

func (vc *vidMap) deleteLocation(vid uint32, location Location) {
	vc.Lock()
	defer vc.Unlock()

	locations, found := vc.vid2Locations[vid]
	if !found {
		return
	}

	for i, loc := range locations {
		if loc.Url == location.Url {
			vc.vid2Locations[vid] = append(locations[0:i], locations[i+1:]...)
			break
		}
	}

}
