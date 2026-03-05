use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    File,
    Dir,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub node_id: String,
    pub tenant_id: String,
    pub parent_id: Option<String>,
    pub name: String,
    pub path: String,
    pub kind: NodeKind,
    pub logical_clock: u64,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileVersion {
    pub version_id: String,
    pub node_id: String,
    pub blob_key: String,
    pub size: u64,
    pub content_hash: Option<String>,
    pub writer_device_id: Option<String>,
    pub committed_at: DateTime<Utc>,
    pub overwrite_of_version_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOp {
    Create,
    Write,
    Rename,
    Delete,
    Restore,
    Mkdir,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub event_id: String,
    pub tenant_id: String,
    pub node_id: String,
    pub op: ChangeOp,
    pub version_id: Option<String>,
    pub ts: DateTime<Utc>,
    pub actor: String,
    pub cursor: u64,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanTier {
    Free,
    Pro,
    Team,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub plan_tier: PlanTier,
    pub max_days: i64,
    pub max_versions: usize,
    pub hard_delete_grace_days: i64,
}

impl RetentionPolicy {
    pub fn free() -> Self {
        Self {
            plan_tier: PlanTier::Free,
            max_days: 7,
            max_versions: 20,
            hard_delete_grace_days: 7,
        }
    }

    pub fn pro() -> Self {
        Self {
            plan_tier: PlanTier::Pro,
            max_days: 30,
            max_versions: 200,
            hard_delete_grace_days: 30,
        }
    }

    pub fn team() -> Self {
        Self {
            plan_tier: PlanTier::Team,
            max_days: 180,
            max_versions: 1000,
            hard_delete_grace_days: 60,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceStartResponse {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub interval_seconds: u64,
    pub expires_in_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevicePollRequest {
    pub device_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevicePollResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in_seconds: u64,
    pub token_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshTokenRequest {
    pub refresh_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshTokenResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in_seconds: u64,
    pub token_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogoutRequest {
    pub refresh_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceActivateRequest {
    pub user_code: String,
    pub user_id: String,
    pub tenant_id: String,
    #[serde(default)]
    pub plan_tier: Option<PlanTier>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadInitRequest {
    pub path: String,
    pub size_hint: Option<u64>,
    pub content_hash: Option<String>,
    pub writer_device_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadInitResponse {
    pub upload_id: String,
    pub blob_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadPartRequest {
    pub upload_id: String,
    pub part_number: u32,
    pub data_base64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadCommitRequest {
    pub upload_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadCommitResponse {
    pub node_id: String,
    pub version_id: String,
    pub cursor: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MkdirRequest {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenameRequest {
    pub from_path: String,
    pub to_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaResponse {
    pub node: Node,
    pub head_version: Option<FileVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub node: Node,
    pub head_version: Option<FileVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDirectoryResponse {
    pub path: String,
    pub entries: Vec<DirectoryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListVersionsResponse {
    pub node_id: String,
    pub versions: Vec<FileVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreVersionResponse {
    pub node_id: String,
    pub restored_version_id: String,
    pub cursor: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncPullResponse {
    pub events: Vec<ChangeEvent>,
    pub next_cursor: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentPlanResponse {
    pub tenant_id: String,
    pub plan_tier: PlanTier,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicyResponse {
    pub tenant_id: String,
    pub policy: RetentionPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyRetentionRequest {
    pub tenant_id: String,
    pub policy: RetentionPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord {
    pub audit_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub action: String,
    pub resource: Option<String>,
    pub outcome: String,
    pub ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditListResponse {
    pub events: Vec<AuditRecord>,
}

#[cfg(test)]
mod tests {
    use super::{DeviceActivateRequest, PlanTier, RetentionPolicy};
    use serde_json::json;

    #[test]
    fn retention_policy_presets_match_expected_limits() {
        let free = RetentionPolicy::free();
        assert_eq!(free.plan_tier, PlanTier::Free);
        assert_eq!(free.max_days, 7);
        assert_eq!(free.max_versions, 20);
        assert_eq!(free.hard_delete_grace_days, 7);

        let pro = RetentionPolicy::pro();
        assert_eq!(pro.plan_tier, PlanTier::Pro);
        assert_eq!(pro.max_days, 30);
        assert_eq!(pro.max_versions, 200);
        assert_eq!(pro.hard_delete_grace_days, 30);

        let team = RetentionPolicy::team();
        assert_eq!(team.plan_tier, PlanTier::Team);
        assert_eq!(team.max_days, 180);
        assert_eq!(team.max_versions, 1000);
        assert_eq!(team.hard_delete_grace_days, 60);
    }

    #[test]
    fn plan_tier_uses_snake_case_json_values() {
        let pro = serde_json::to_string(&PlanTier::Pro).expect("serialize plan tier");
        let team = serde_json::to_string(&PlanTier::Team).expect("serialize plan tier");

        assert_eq!(pro, "\"pro\"");
        assert_eq!(team, "\"team\"");
    }

    #[test]
    fn device_activate_request_defaults_plan_tier_to_none() {
        let req: DeviceActivateRequest = serde_json::from_value(json!({
            "user_code": "abc123",
            "user_id": "u1",
            "tenant_id": "t1"
        }))
        .expect("deserialize request");

        assert!(req.plan_tier.is_none());
    }

    #[test]
    fn device_activate_request_accepts_plan_tier_when_present() {
        let req: DeviceActivateRequest = serde_json::from_value(json!({
            "user_code": "abc123",
            "user_id": "u1",
            "tenant_id": "t1",
            "plan_tier": "free"
        }))
        .expect("deserialize request");

        assert_eq!(req.plan_tier, Some(PlanTier::Free));
    }
}
