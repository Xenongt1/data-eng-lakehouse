resource "aws_athena_workgroup" "lakehouse" {
  name        = "lakehouse_wg"
  description = "Workgroup for ad-hoc analytical queries against the silver-zone Delta tables."
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  # Default `force_destroy = false`: refuse to delete if there are stored
  # queries — they're cheap insurance against accidental loss.
}
