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
		State:          req.State,
		Completed:      req.Completed,
	}

	stage_taskid := strconv.FormatInt(int64(req.Stage), 10) + "_" + strconv.FormatInt(int64(req.TaskId), 10)
	// check which stage the checkpoint is for
	NodeCheckpointStats.mutex.RLock()
	existingStats, exists := NodeCheckpointStats.stats[req.Vmname][stage_taskid]
	NodeCheckpointStats.mutex.RUnlock()

	if exists && existingStats.TempFilename != "" && existingStats.TempFilename != req.Filename {
		errMsg := fmt.Sprintf("error: filename mismatch for vm %s, stage %d. expected: %s, recv: %s",
			req.Vmname, req.Stage, existingStats.TempFilename, req.Filename)
		utility.LogMessage(errMsg)
	}

	// update the checkpoint stats of that node, using the stage value as key
	UpdateNodeCheckpointStats(req.Vmname, stage_taskid, tempStats)

	response_prev_stage_over := false
	if req.Completed {
		utility.LogMessage("Received completed message for VM: " + req.Vmname + ", Stage_TaskId: " + stage_taskid)
		if req.Stage == 0 {
			// stage 0 says completed
			// cleanup data structures
			CleanUpTaskCompletion(req.Vmname, req.Stage, req.TaskId, req.Operation)
			response_prev_stage_over = true
		} else if req.Stage == 1 || req.Stage == 2 {
			// check if prev stage has any running tasks (stage 0, or stage 1)
			if len(GetTasksForStage(int32(req.Stage-1))) == 0 {
				// perform cleanup, set
				CleanUpTaskCompletion(req.Vmname, req.Stage, req.TaskId, req.Operation)
				response_prev_stage_over = true
			} else {
				prev_stage_len := len(GetTasksForStage(int32(req.Stage - 1)))
				utility.LogMessage(fmt.Sprintf("Prev stage not over, waiting for completion - %d\n", prev_stage_len))
				for _, task := range GetTasksForStage(int32(req.Stage - 1)) {
					utility.LogMessage(fmt.Sprintf("Task: %d, %d\n", task.TaskID, task.Node))
				}
				response_prev_stage_over = false
			}
		}

	}

	utility.LogMessage("updated checkpoint stats for VM: " + req.Vmname + ", Stage_TaskId: " + stage_taskid + ", Lines Processed: " + strconv.FormatInt(int64(req.LineRangeProcessed), 10) + ", Filename: " + req.Filename)

	// send ack as the line number saved for that node[stage]
	// prev stage completed
	response := &stormgrpc.AckCheckpoint{
		LineAcked:          req.LineRangeProcessed,
		PrevStageCompleted: response_prev_stage_over,
	}

	utility.LogMessage("sent checkpoint ack for VM: " + req.Vmname + ", Stage_TaskId: " + stage_taskid + ", Lines Processed: " + strconv.FormatInt(int64(req.LineRangeProcessed), 10))

	return response, nil
}
