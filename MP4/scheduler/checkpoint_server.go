package scheduler

import (
	"fmt"
	"rainstorm/stormgrpc"
	"rainstorm/utility"
	"strconv"
)

type CheckpointServer struct {
	stormgrpc.UnimplementedCheckpointServiceServer
}

func (s *CheckpointServer) Checkpoint(req *stormgrpc.CheckpointRequest, stream stormgrpc.CheckpointService_CheckpointServer) error {
	utility.LogMessage("RPC Checkpoint server entered - " + req.Vmname)
	// Accept request
	utility.LogMessage("RPC Checkpoint server entered - " + req.Vmname)

	// Accept request
	utility.LogMessage(fmt.Sprintf("Received checkpoint request for VM: %s, Stage: %s", req.Vmname, req.Stage))

	// create temp var for checkpoint stats
	tempStats := CheckpointStats{
		Stage:          req.Stage,
		LinesProcessed: int(req.LineRangeProcessed),
		TempFilename:   req.Filename,
	}

	// check which stage the checkpoint is for
	NodeCheckpointStats.mutex.RLock()
	existingStats, exists := NodeCheckpointStats.stats[req.Vmname][req.Stage]
	NodeCheckpointStats.mutex.RUnlock()

	if exists && existingStats.TempFilename != req.Filename {
		errMsg := fmt.Errorf("error: filename mismatch for vm %s, stage %s. expected: %s, recv: %s",
			req.Vmname, req.Stage, existingStats.TempFilename, req.Filename)
		utility.LogMessage(errMsg.Error())
		return errMsg
	}

	// update the checkpoint stats of that node, using the stage value as key
	UpdateNodeCheckpointStats(req.Vmname, req.Stage, tempStats)

	utility.LogMessage("updated checkpoint stats for VM: " + req.Vmname + ", Stage: " + req.Stage + ", Lines Processed: " + strconv.FormatInt(req.LineRangeProcessed, 10) + ", Filename: " + req.Filename)

	// send ack as the line number saved for that node[stage]
	ack := &stormgrpc.AckCheckpoint{
		LineAcked: req.LineRangeProcessed,
	}

	if err := stream.Send(ack); err != nil {
		utility.LogMessage("Error sending checkpoint ack: " + err.Error())
		return err
	}

	utility.LogMessage("sent checkpoint ack for VM: " + req.Vmname + ", Stage: " + req.Stage + ", Lines Processed: " + strconv.FormatInt(req.LineRangeProcessed, 10))

	return nil
}
