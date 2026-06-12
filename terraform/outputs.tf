output "bucket_name" {
  description = "Name of the lakehouse S3 bucket."
  value       = aws_s3_bucket.lakehouse.bucket
}

output "bucket_arn" {
  description = "ARN of the lakehouse S3 bucket."
  value       = aws_s3_bucket.lakehouse.arn
}

output "glue_database_name" {
  description = "Glue Catalog database holding the silver-zone tables."
  value       = aws_glue_catalog_database.lakehouse_dwh.name
}

output "glue_job_names" {
  description = "Names of the three PySpark Glue jobs."
  value       = [for j in aws_glue_job.jobs : j.name]
}

output "glue_crawler_name" {
  description = "Name of the Glue crawler that refreshes the catalog."
  value       = aws_glue_crawler.lakehouse.name
}

output "glue_role_arn" {
  description = "ARN of the IAM role assumed by Glue jobs and the crawler."
  value       = aws_iam_role.glue.arn
}

output "sfn_role_arn" {
  description = "ARN of the IAM role assumed by Step Functions."
  value       = aws_iam_role.sfn.arn
}

output "events_role_arn" {
  description = "ARN of the IAM role EventBridge uses to start Step Functions executions."
  value       = aws_iam_role.events.arn
}

output "account_id" {
  description = "AWS account ID Terraform is deploying into."
  value       = local.account_id
}

output "region" {
  description = "AWS region Terraform is deploying into."
  value       = local.region
}

output "state_machine_arn" {
  description = "ARN of the lakehouse ingest Step Functions state machine."
  value       = aws_sfn_state_machine.lakehouse.arn
}

output "state_machine_name" {
  description = "Name of the lakehouse ingest Step Functions state machine."
  value       = aws_sfn_state_machine.lakehouse.name
}

output "event_rule_name" {
  description = "EventBridge rule that fans S3 object-created events into Step Functions."
  value       = aws_cloudwatch_event_rule.raw_object_created.name
}

output "athena_workgroup" {
  description = "Athena workgroup to use for analytical queries."
  value       = aws_athena_workgroup.lakehouse.name
}

output "athena_results_location" {
  description = "S3 location where Athena writes query results."
  value       = "s3://${aws_s3_bucket.lakehouse.bucket}/athena-results/"
}
