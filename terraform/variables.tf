variable "region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Used as a prefix on all resource names and as a tag value."
  type        = string
  default     = "lakehouse-ecommerce"
}

variable "environment" {
  description = "Deployment environment (dev/stg/prod). Used in tags."
  type        = string
  default     = "dev"
}

variable "bucket_name" {
  description = "Override for the S3 bucket name. If null, computed as <project>-<account>-<region> for global uniqueness."
  type        = string
  default     = null
}

variable "glue_version" {
  description = "Glue runtime version. 4.0 ships with Delta 2.x out of the box."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type. G.1X is plenty for the sample data; bump for prod."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Worker count per Glue job. Two is enough for the sample data."
  type        = number
  default     = 2
}

variable "glue_job_timeout_minutes" {
  description = "Glue job timeout (minutes). 30 is safe for the sample data."
  type        = number
  default     = 30
}
