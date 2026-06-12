resource "aws_glue_catalog_database" "lakehouse_dwh" {
  name        = "lakehouse_dwh"
  description = "Silver-zone Delta tables for the e-commerce lakehouse."
}

locals {
  glue_jobs = {
    process_products    = "process_products.py"
    process_orders      = "process_orders.py"
    process_order_items = "process_order_items.py"
  }
}

resource "aws_glue_job" "jobs" {
  for_each = local.glue_jobs

  name              = "${var.project_name}-${each.key}"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = 0
  timeout           = var.glue_job_timeout_minutes

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/scripts/${each.value}"
  }

  default_arguments = {
    "--datalake-formats"                 = "delta"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--TempDir"                          = "s3://${aws_s3_bucket.lakehouse.bucket}/temp/"
    "--bucket"                           = aws_s3_bucket.lakehouse.bucket
    # source_key is supplied at run time by Step Functions; placeholder lets
    # the job start manually for smoke tests.
    "--source_key" = ""
  }

  execution_property {
    max_concurrent_runs = 5
  }

  depends_on = [
    aws_iam_role_policy.glue_s3,
    aws_iam_role_policy_attachment.glue_service,
  ]
}

resource "aws_glue_crawler" "lakehouse" {
  name          = "${var.project_name}-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.lakehouse_dwh.name

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/processed/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}
