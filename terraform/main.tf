terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = ">= 5.0"
    }
  }
}

# Configuração Global do Provider
provider "google" {
  project = "ghm-data-prod"
  region  = "us-central1"
}

provider "google-beta" {
  project = "ghm-data-prod"
  region  = "us-central1"
}

# --- IAM e Service Accounts ---

resource "google_service_account" "composer_sa" {
  account_id   = "composer-env-sa"
  display_name = "Service Account para o Composer Environment"
  project      = "ghm-data-prod"
}

resource "google_project_service_identity" "composer_agent" {
  provider = google-beta
  service  = "composer.googleapis.com"
  project  = "ghm-data-prod"
}

resource "google_project_iam_member" "composer_worker" {
  project = "ghm-data-prod"
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_service_account_iam_member" "composer_agent_auth" {
  provider           = google-beta
  service_account_id = google_service_account.composer_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_project_service_identity.composer_agent.email}"
}

# --- Storage ---

resource "google_storage_bucket" "composer_bucket" {
  name                        = "ghm-data-prod-composer-bucket-001"
  location                    = "US-CENTRAL1"
  force_destroy               = true 
  uniform_bucket_level_access = true
}

# --- Composer ---

resource "google_composer_environment" "airflow_composer_3" {
  name    = "ghm-composer-env-3"
  region  = "us-central1"
  project = "ghm-data-prod" # Reforçando o projeto explicitamente no recurso principal

  storage_config {
    bucket = google_storage_bucket.composer_bucket.name
  }

  config {
    software_config {
      image_version = "composer-3-airflow-2.10.2"
    }

    node_config {
      service_account = google_service_account.composer_sa.email
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }
    }
  }

  depends_on = [
    google_project_iam_member.composer_worker,
    google_service_account_iam_member.composer_agent_auth,
    google_storage_bucket.composer_bucket
  ]
}