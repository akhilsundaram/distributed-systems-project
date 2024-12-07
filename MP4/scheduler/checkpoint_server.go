package scheduler

import (
	"context"
	"fmt"
	"rainstorm/stormgrpc"
	"rainstorm/utility"
	"strconv"
)

type CheckpointServer struct {
	stormgrpc.UnimplementedCheckpointServiceServer
}

func (s *CheckpointServer) Checkpoint(ctx context.Context, req *stormgrpc.CheckpointRequest) (*stormgrpc.AckCheckpoint, error) {
	utility.LogMessage("RPC Checkpoint server entered - " + req.Vmname)

	// Accept request for checkpoint
	utility.LogMessage(fmt.Sprintf("Received checkpoint request for VM: %s, Stage: %d, TaskId: %d, Lines Processed: %d", req.Vmname, req.Stage, req.TaskId, req.LineRangeProcessed))

	// create temp var for checkpoint stats
	tempStats := CheckpointStats{
		Stage:          req.Stage,
		LinesProcessed: req.LineRangeProcessed,
		TempFilename:   req.Filename,
		Operation:      req.Operation,
		TaskId:         req.TaskId,
		VmName:         req.Vmname,
	}

	stage_taskid := strconv.FormatInt(int64(req.Stage), 10) + "_" + strconv.FormatInt(int64(req.TaskId), 10)
	// check which stage the checkpoint is for
	NodeCheckpointStats.mutex.RLock()
	existingStats, exists := NodeCheckpointStats.stats[req.Vmname][stage_taskid]
	NodeCheckpointStats.mutex.RUnlock()

	if exists && existingStats.TempFilename != req.Filename {
		errMsg := fmt.Errorf("error: filename mismatch for vm %s, stage %d. expected: %s, recv: %s",
			req.Vmname, req.Stage, existingStats.TempFilename, req.Filename)
		utility.LogMessage(errMsg.Error())
		return nil, errMsg
	}

	// update the checkpoint stats of that node, using the stage value as key
	UpdateNodeCheckpointStats(req.Vmname, stage_taskid, tempStats)

	utility.LogMessage("updated checkpoint stats for VM: " + req.Vmname + ", Stage_TaskId: " + stage_taskid + ", Lines Processed: " + strconv.FormatInt(req.LineRangeProcessed, 10) + ", Filename: " + req.Filename)

	// send ack as the line number saved for that node[stage]
	response := &stormgrpc.AckCheckpoint{
		LineAcked: req.LineRangeProcessed,
	}

	utility.LogMessage("sent checkpoint ack for VM: " + req.Vmname + ", Stage_TaskId: " + stage_taskid + ", Lines Processed: " + strconv.FormatInt(req.LineRangeProcessed, 10))

	return response, nil
}
