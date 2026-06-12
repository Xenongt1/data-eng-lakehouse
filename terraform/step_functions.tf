resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/lakehouse"
  retention_in_days = 30
}

locals {
  state_machine_definition = templatefile(
    "${path.module}/../step_functions/state_machine.asl.json",
    {
      products_job    = aws_glue_job.jobs["process_products"].name
      orders_job      = aws_glue_job.jobs["process_orders"].name
      order_items_job = aws_glue_job.jobs["process_order_items"].name
      crawler_name    = aws_glue_crawler.lakehouse.name
    }
  )
}

resource "aws_sfn_state_machine" "lakehouse" {
  name     = "${var.project_name}-ingest"
  role_arn = aws_iam_role.sfn.arn

  definition = local.state_machine_definition

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  depends_on = [
    aws_iam_role_policy.sfn,
  ]
}

###############################################################################
# EventBridge: route S3 object-created events under raw/ to the state machine.
###############################################################################

resource "aws_cloudwatch_event_rule" "raw_object_created" {
  name        = "${var.project_name}-raw-object-created"
  description = "Fires when a new object is created under raw/ in the lakehouse bucket."

  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = {
        name = [aws_s3_bucket.lakehouse.bucket]
      }
      object = {
        key = [{ prefix = "raw/" }]
      }
    }
  })

  depends_on = [aws_s3_bucket_notification.lakehouse]
}

resource "aws_cloudwatch_event_target" "to_sfn" {
  rule      = aws_cloudwatch_event_rule.raw_object_created.name
  target_id = "lakehouse-sfn"
  arn       = aws_sfn_state_machine.lakehouse.arn
  role_arn  = aws_iam_role.events.arn
}
