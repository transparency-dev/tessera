include "root" {
  path   = find_in_parent_folders()
  expose = true
}

inputs = merge(
  include.root.locals,
  {
    # Service accounts are managed externally.
    service_account = "cloudbuild-${include.root.locals.env}-sa@trillian-tessera.iam.gserviceaccount.com"
  }
)
