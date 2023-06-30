// Copyright 2016 Diamanti Inc
//
// Author: Yatish Devadiga
//
// This file defines library functions and APIs for Diamanti Volume Snapshots

package lib

import (
	"gitlab.eng.diamanti.com/software/main.git/convoy/api"

	. "gitlab.eng.diamanti.com/software/main.git/test/lib/coreutils"

	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	KMaxSnapshotIters      int = 600
	KMaxSnapshotsPerVolume int = api.KMaxSnapshots
)

// Helper to create snapshot
func snapshotCreateCmd(cmd string) (*api.Snapshot, error) {
	if err, out, _ := LexecJsonOut(cmd); err != nil {
		DebugDepth(1, "Failed snapshot create: %s\n", err.Error())
		return nil, err
	} else {
		if out.Len() == 0 {
			Debug("Empty output on snapshot create\n")
			return nil, (fmt.Errorf("Empty output on snapshot create"))
		}

		var result api.Snapshot
		if err := json.Unmarshal(out.Bytes(), &result); err != nil {
			Debug("Error %s unmarshaling json data on snapshot create: %s\n", err.Error(), out.String())
			return nil, err
		} else {
			return &result, nil
		}
	}
}

// Helper to list snapshots
func snapshotListCmd(cmd string) (*api.SnapshotList, error) {
	var err error
	for iters := 0; iters < KMaxIters; iters++ {
		if err, out, _ := LexecJsonOut(cmd); err != nil {
			return nil, err
		} else {
			var result api.SnapshotList
			if out.Len() == 0 {
				Debug("Empty output on snapshot list\n")
				err = errors.New("Empty output on snapshot list")
			} else if err := json.Unmarshal(out.Bytes(), &result); err != nil {
				Debug("Error %s unmarshaling json data on snapshot list: %s\n", err.Error(), out.String())
				return nil, err
			} else {
				return &result, nil
			}
		}
		time.Sleep(KPollTimeStep)
	}
	return nil, err
}

// Helper to get snapshot
func snapshotGetCmd(cmd string) (*api.Snapshot, error) {
	if err, out, _ := LexecJsonOut(cmd); err != nil {
		return nil, err
	} else {
		if out.Len() == 0 {
			Debug("Empty output on snapshot get\n")
			return nil, (fmt.Errorf("Empty output on snapshot get"))
		} else {
			var result api.Snapshot
			if err := json.Unmarshal(out.Bytes(), &result); err != nil {
				Debug("Error %s unmarshaling json data on snapshot get: %s\n",
					err.Error(), out.String())
				return nil, err
			} else {
				return &result, nil
			}
		}
	}
}

// Helper to delete snapshot
func snapshotDeleteCmd(cmd string) error {
	if err, out, _ := LexecJsonOut(cmd); err != nil {
		DebugDepth(1, "Error %s deleting snapshot, cmd:%s out: %v\n",
			err.Error(), cmd, out)
		return err
	} else {
		// Parse the returned string to see if delete succeeded
		var result api.VolumeStatus
		if out.Len() == 0 {
			Debug("Empty output on snapshot delete\n")
			err = errors.New("Empty output on snapshot delete")
			return err
		} else if err := json.Unmarshal(out.Bytes(), &result); err != nil {
			Debug("Error %s unmarshaling json data on snapshot delete: %s\n",
				err.Error(), out.String())
			return err
		} else {
			return nil
		}
	}
}

// Helper to wait for state of snapshot or return error after timeout.
func WaitForSnapshotState(snapshot_name, state string, timeoutSecs int) error {
	for iters := 0; iters < timeoutSecs; iters++ {
		snapshot, err := SnapshotGet(snapshot_name)
		if err != nil {
			return err
		}
		if string(snapshot.Status.State) == state {
			return nil
		} else {
			time.Sleep(time.Second * 1)
		}
	}
	errMsg := fmt.Sprintf("Timeout while waiting for snapshot %s to change to %s state", snapshot_name, state)
	return errors.New(errMsg)
}

type SnapshotCmd struct {
	SnapshotName string
	Cmd          string
}

func SnapshotCreateRequest(name string) *SnapshotCmd {
	var sc SnapshotCmd
	sc.Cmd = " snapshot create "
	sc.Cmd += name
	return &sc
}

func (sc *SnapshotCmd) Source(sourceVolume string) *SnapshotCmd {
	if sourceVolume != "" {
		sc.Cmd += " --src " + sourceVolume
	}
	return sc
}

func (sc *SnapshotCmd) MirrorCount(mirrorCount string) *SnapshotCmd {
	if mirrorCount != "" {
		sc.Cmd += " --mirror-count " + mirrorCount
	}
	return sc
}

func (sc *SnapshotCmd) Selector(nodeLabel string) *SnapshotCmd {
	if nodeLabel != "" {
		sc.Cmd += " --selectors " + nodeLabel
	}
	return sc
}

func (sc *SnapshotCmd) Prefix(prefix string) *SnapshotCmd {
	if prefix != "" {
		sc.Cmd += " --prefix " + prefix
	}
	return sc
}

func (sc *SnapshotCmd) Create() error {
	if snapshot, err := snapshotCreateCmd(sc.Cmd); err == nil {
		return WaitForSnapshotState(snapshot.Name, "Available", kMaxVolumeTimeout)
	} else {
		return err
	}
}

// List all snapshots for a given volume
func SnapshotListForVolume(volName string) (*api.SnapshotList, error) {
	cmd := "snapshot list -s " + volName
	return (snapshotListCmd(cmd))
}

// List all snapshots for all volumes
func SnapshotList() (*api.SnapshotList, error) {
	cmd := "snapshot list"
	return (snapshotListCmd(cmd))
}

// Get snapshot info.
func SnapshotGet(name string) (*api.Snapshot, error) {
	cmd := "snapshot get " + name
	if snapshot, err := snapshotGetCmd(cmd); err != nil {
		return nil, err
	} else {
		return snapshot, nil
	}
}

func WaitForSnapshotDeletion(snapshotName string) error {
	const (
		snapshotDeleteTimeout = 1200 // 1200 seconds
	)

	t := time.Now()
	for ((time.Now()).Sub(t)).Seconds() < snapshotDeleteTimeout {
		if _, err := SnapshotGet(snapshotName); err != nil {
			expectedError := "Snapshot " + "\\\"" + snapshotName + "\\\"" + " not found"
			if strings.Contains(err.Error(), expectedError) {
				return nil
			}
		}
		time.Sleep(1 * time.Second)
	}
	return errors.New(fmt.Sprintf("Error: timeout while waiting for snapshot \"%s\" to delete.", snapshotName))
}

// Delete snapshot given its name.
func SnapshotDelete(name string) error {

	cmd := "snapshot delete --assume-yes " + name
	err := snapshotDeleteCmd(cmd)
	if err != nil {
		return err
	}

	// Wait for snapshot to get deleted
	for iters := 0; iters < KMaxIters; iters++ {
		if _, err := SnapshotGet(name); err != nil {
			expected_error := "Snapshot " + "\\\"" + name + "\\\"" + " not found"
			if strings.Contains(err.Error(), expected_error) {
				//Success
				return nil
			} else {
				Debug("Unexpected error: expected error: %s, got : %s",
					expected_error, err.Error())
				return err
			}
		}
		time.Sleep(KPollTimeStep)
	}

	return fmt.Errorf("Delete of snapshot %s timed out", name)
}

// Wrapper to create N snapshots
func CreateNSnapshots(baseVolumeName, baseSnapshotName string, numSnapshots int, nodeLabel, prefix string, mirrorCnt int) error {

	mirrors := ""
	if mirrorCnt > 0 {
		mirrors = strconv.Itoa(mirrorCnt)
		Debug("Mirror Count: %s", mirrors)
	}
	for cnt := 1; cnt <= numSnapshots; cnt++ {
		volumeName := baseVolumeName + strconv.Itoa(cnt)
		snapshotName := baseSnapshotName + strconv.Itoa(cnt)
		err := SnapshotCreateRequest(snapshotName).Source(volumeName).Selector(nodeLabel).MirrorCount(mirrors).Prefix(prefix).Create()
		if err != nil {
			return err
		}
	}
	return nil
}

// Function creates N snapshots of the given volume.
func CreateNSnapshotOutOfVolume(snapshotNamePrefix, volumeName, nodeLabel, prefix string, numSnapshots, mirrorCount int) ([]string, error) {

	var (
		snapshotNameList []string
	)

	Debug("Creating %d snapshots out of volume \"%s\"", numSnapshots, volumeName)
	for cnt := 1; cnt <= numSnapshots; cnt++ {
		snapshotName := snapshotNamePrefix + "-" + volumeName + "-" + strconv.Itoa(cnt)
		err := SnapshotCreateRequest(snapshotName).Source(volumeName).Selector(nodeLabel).MirrorCount(strconv.Itoa(mirrorCount)).Prefix(prefix).Create()
		if err != nil {
			return snapshotNameList, err
		}

		snapshotNameList = append(snapshotNameList, snapshotName)
	}
	return snapshotNameList, nil
}

// Wrapper to create snapshots of N volumes
func CreateNVolumesFromSnapshot(base_vol_name_from_snapshot, base_snapshot_name string, nvols int, node_label string, mirror_cnt int, vol_label string) error {

	mirrors := ""
	if mirror_cnt > 0 {
		mirrors = strconv.Itoa(mirror_cnt)
		Debug("Mirror Count: %s", mirrors)
	}

	for cnt := 1; cnt <= nvols; cnt++ {
		vol_name_from_snapshot := base_vol_name_from_snapshot + strconv.Itoa(cnt)
		snapshot_name := base_snapshot_name + strconv.Itoa(cnt)
		err := NewVolumeCreateRequest(vol_name_from_snapshot).SourceVol(snapshot_name).Selector(node_label).MirrorCount(mirrors).Label(vol_label).Create()
		if err != nil {
			return err
		}
	}
	return nil
}

// Wrapper to delete snapshots of N volumes
func DeleteNSnapshots(baseSnapshotName string, numOfSnaps int, waitTillDeleteComplete bool) error {

	for cnt := 1; cnt <= numOfSnaps; cnt++ {
		snapshotName := baseSnapshotName + strconv.Itoa(cnt)
		cmd := "snapshot delete --assume-yes " + snapshotName
		err := snapshotDeleteCmd(cmd)
		if err != nil {
			return err
		}
	}
	if waitTillDeleteComplete {
		return WaitForAllSnapshotDelete(baseSnapshotName, numOfSnaps)
	}
	return nil
}

func WaitForAllSnapshotDelete(baseSnapshotName string, numOfSnaps int) error {

	for cnt := 1; cnt <= numOfSnaps; cnt++ {
		snapshotName := baseSnapshotName + strconv.Itoa(cnt)
		deleted := false
		// Wait for snapshot to get deleted
		for iters := 0; iters < KMaxIters; iters++ {
			err := WaitForSnapshotDeletion(snapshotName)
			if err != nil {
				return err
			} else {
				deleted = true
				break
			}
		}
		if !deleted {
			return fmt.Errorf("Delete of snapshot %s timed out", snapshotName)
		}
	}
	return nil
}

// Function will delete all snapshots(if respective LCVs are already deleted) in a cluster
func DeleteAllSnapshots() error {

	snapshotList, err := SnapshotList()
	if err != nil {
		return err
	}

	for _, snapshot := range snapshotList.Items {
		DebugDepth(1, "Deleting snapshot \"%s\"", snapshot.Name)
		cmd := "snapshot delete --assume-yes " + snapshot.Name
		if err = snapshotDeleteCmd(cmd); err == nil {
			continue
		} else {
			//WORKAROUND FOR BUG: DWS-5298
			if strings.Contains(err.Error(), "Multioperation transaction failed for txn map") {
				time.Sleep(1 * time.Second)
				for idx := 0; idx < 10; idx++ {
					if err = snapshotDeleteCmd(cmd); err == nil {
						break
					} else if strings.Contains(err.Error(), "Multioperation transaction failed for txn map") {
						err = snapshotDeleteCmd(cmd)
						time.Sleep(1 * time.Second)
					} else {
						return err
					}
				}
			} else {
				return err
			}
		}
	}

	// Wait for snapshot to get deleted
	for _, snapshot := range snapshotList.Items {

		snapshot_name := snapshot.Name
		deleted := false
		for iters := 0; iters < KMaxSnapshotIters; iters++ {
			if _, err := SnapshotGet(snapshot_name); err != nil {
				expected_error := "Snapshot " + "\\\"" + snapshot_name + "\\\"" + " not found"
				if strings.Contains(err.Error(), expected_error) {
					deleted = true
					break
				} else {
					Debug("Unexpected error: expected error: %s, got : %s",
						expected_error, err.Error())
					return err
				}
			}
			time.Sleep(KPollTimeStep)
		}

		if !deleted {
			return fmt.Errorf("Delete of snapshot %s timed out", snapshot_name)
		}
	}
	return nil
}

