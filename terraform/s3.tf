resource "aws_s3_bucket" "lakehouse" {
  bucket        = local.bucket_name
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# EventBridge notifications must be turned on explicitly; the S3 → Step Functions
# rule in step_functions.tf depends on this.
resource "aws_s3_bucket_notification" "lakehouse" {
  bucket      = aws_s3_bucket.lakehouse.id
  eventbridge = true
}

# Create the six top-level prefixes as empty objects so the layout is visible
# in the console before any data lands. These are placeholders, not data.
locals {
  bucket_prefixes = [
    "raw/",
    "processed/",
    "archive/",
    "quarantine/",
    "scripts/",
    "athena-results/",
  ]
}

resource "aws_s3_object" "prefixes" {
  for_each = toset(local.bucket_prefixes)

  bucket  = aws_s3_bucket.lakehouse.id
  key     = each.value
  content = ""
}
