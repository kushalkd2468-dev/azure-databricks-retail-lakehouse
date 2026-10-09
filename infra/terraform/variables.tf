variable "project" {
  description = "Short lowercase name used in resource names (letters/digits only, <= 8 chars)"
  type        = string
  default     = "retaillh"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "centralindia"
}

variable "adf_trigger_start_time" {
  description = "First run of the daily ADF trigger (UTC). 20:30Z = 02:00 IST next day."
  type        = string
  default     = "2026-10-12T20:30:00Z"
}
