package com.alstom.datalab.hadoop

/**
  * Created by raphael on 14/12/2015.
  */
import org.apache.hadoop.mapred.JobContext
import org.apache.hadoop.mapred.OutputCommitter
import org.apache.hadoop.mapred.TaskAttemptContext

/**
  * @author ssabarish
  *
  */
class DirectOutputCommitter extends OutputCommitter {

  override def abortTask(task: TaskAttemptContext):Unit = return

  override def commitTask(task: TaskAttemptContext):Unit = return

  override def needsTaskCommit(task: TaskAttemptContext):Boolean = false

  override def setupJob(job: JobContext):Unit = return

  override def setupTask(task: TaskAttemptContext):Unit = return

}
