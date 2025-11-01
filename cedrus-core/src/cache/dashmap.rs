use std::collections::HashMap;

use dashmap::DashMap;
use cedrus_cedar::{Entity, EntityUid, Policy, PolicyId, PolicySet, Schema, Template, TemplateLink};
use uuid::Uuid;

use crate::core::{project::Project, IdentitySource};

use super::{Cache, CacheError};

pub struct DashMapCache {
    projects: DashMap<Uuid, Project>,
    identity_sources: DashMap<Uuid, IdentitySource>,
    schemas: DashMap<Uuid, Schema>,
    entities: DashMap<(Uuid, EntityUid), Entity>,
    policies: DashMap<(Uuid, PolicyId), Policy>,
    templates: DashMap<(Uuid, PolicyId), Template>,
    template_links: DashMap<(Uuid, PolicyId), TemplateLink>,
}

impl DashMapCache {
    pub fn new() -> Self {
        Self {
            projects: DashMap::new(),
            identity_sources: DashMap::new(),
            schemas: DashMap::new(),
            entities: DashMap::new(),
            policies: DashMap::new(),
            templates: DashMap::new(),
            template_links: DashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl Cache for DashMapCache {
    async fn project_clear(&self, project_id: &Uuid) -> Result<(), CacheError> {
        self.projects.remove(project_id);
        self.identity_sources.remove(project_id);
        self.schemas.remove(project_id);
        self.entities.retain(|(pid, _), _| pid != project_id);
        self.policies.retain(|(pid, _), _| pid != project_id);
        self.templates.retain(|(pid, _), _| pid != project_id);
        self.template_links.retain(|(pid, _), _| pid != project_id);
        Ok(())
    }

    async fn projects_get(&self) -> Result<Vec<Project>, CacheError> {
        Ok(self.projects.iter().map(|r| r.value().clone()).collect())
    }

    async fn project_get(&self, project_id: &Uuid) -> Result<Option<Project>, CacheError> {
        Ok(self.projects.get(project_id).map(|r| r.value().clone()))
    }

    async fn project_set(&self, project: &Project) -> Result<(), CacheError> {
        self.projects.insert(project.id, project.clone());
        Ok(())
    }

    async fn project_del(&self, project_id: &Uuid) -> Result<(), CacheError> {
        self.projects.remove(project_id);
        Ok(())
    }

    async fn project_get_identity_source(&self, project_id: &Uuid) -> Result<Option<IdentitySource>, CacheError> {
        Ok(self.identity_sources.get(project_id).map(|r| r.value().clone()))
    }

    async fn project_set_identity_source(&self, project_id: &Uuid, identity_source: &IdentitySource) -> Result<(), CacheError> {
        self.identity_sources.insert(*project_id, identity_source.clone());
        Ok(())
    }

    async fn project_del_identity_source(&self, project_id: &Uuid) -> Result<(), CacheError> {
        self.identity_sources.remove(project_id);
        Ok(())
    }

    async fn project_get_schema(&self, project_id: &Uuid) -> Result<Option<Schema>, CacheError> {
        Ok(self.schemas.get(project_id).map(|r| r.value().clone()))
    }

    async fn project_set_schema(&self, project_id: &Uuid, schema: &Schema) -> Result<(), CacheError> {
        self.schemas.insert(*project_id, schema.clone());
        Ok(())
    }

    async fn project_del_schema(&self, project_id: &Uuid) -> Result<(), CacheError> {
        self.schemas.remove(project_id);
        Ok(())
    }

    async fn project_get_entities(&self, project_id: &Uuid, entity_uids: &[EntityUid]) -> Result<Vec<Entity>, CacheError> {
        if entity_uids.is_empty() {
            Ok(self.entities.iter()
                .filter(|r| r.key().0 == *project_id)
                .map(|r| r.value().clone())
                .collect())
        } else {
            Ok(entity_uids.iter()
                .filter_map(|uid| self.entities.get(&(*project_id, uid.clone())).map(|r| r.value().clone()))
                .collect())
        }
    }

    async fn project_set_entities(&self, project_id: &Uuid, entities: &[Entity]) -> Result<(), CacheError> {
        for entity in entities {
            self.entities.insert((*project_id, entity.uid().clone()), entity.clone());
        }
        Ok(())
    }

    async fn project_del_entities(&self, project_id: &Uuid, entity_uids: &[EntityUid]) -> Result<(), CacheError> {
        for uid in entity_uids {
            self.entities.remove(&(*project_id, uid.clone()));
        }
        Ok(())
    }

    async fn project_get_policies(&self, project_id: &Uuid) -> Result<HashMap<PolicyId, Policy>, CacheError> {
        Ok(self.policies.iter()
            .filter(|r| r.key().0 == *project_id)
            .map(|r| (r.key().1.clone(), r.value().clone()))
            .collect())
    }

    async fn project_set_policies(&self, project_id: &Uuid, policies: &HashMap<PolicyId, Policy>) -> Result<(), CacheError> {
        for (policy_id, policy) in policies {
            self.policies.insert((*project_id, policy_id.clone()), policy.clone());
        }
        Ok(())
    }

    async fn project_del_policies(&self, project_id: &Uuid, policy_ids: &[PolicyId]) -> Result<(), CacheError> {
        for policy_id in policy_ids {
            self.policies.remove(&(*project_id, policy_id.clone()));
        }
        Ok(())
    }

    async fn project_get_templates(&self, project_id: &Uuid) -> Result<HashMap<PolicyId, Template>, CacheError> {
        Ok(self.templates.iter()
            .filter(|r| r.key().0 == *project_id)
            .map(|r| (r.key().1.clone(), r.value().clone()))
            .collect())
    }

    async fn project_set_templates(&self, project_id: &Uuid, templates: &HashMap<PolicyId, Template>) -> Result<(), CacheError> {
        for (policy_id, template) in templates {
            self.templates.insert((*project_id, policy_id.clone()), template.clone());
        }
        Ok(())
    }

    async fn project_del_templates(&self, project_id: &Uuid, policy_ids: &[PolicyId]) -> Result<(), CacheError> {
        for policy_id in policy_ids {
            self.templates.remove(&(*project_id, policy_id.clone()));
        }
        Ok(())
    }

    async fn project_get_template_links(&self, project_id: &Uuid) -> Result<Vec<TemplateLink>, CacheError> {
        Ok(self.template_links.iter()
            .filter(|r| r.key().0 == *project_id)
            .map(|r| r.value().clone())
            .collect())
    }

    async fn project_set_template_links(&self, project_id: &Uuid, template_links: &[TemplateLink]) -> Result<(), CacheError> {
        for link in template_links {
            self.template_links.insert((*project_id, link.new_id.clone()), link.clone());
        }
        Ok(())
    }

    async fn project_del_template_links(&self, project_id: &Uuid, policy_ids: &[PolicyId]) -> Result<(), CacheError> {
        for policy_id in policy_ids {
            self.template_links.remove(&(*project_id, policy_id.clone()));
        }
        Ok(())
    }

    async fn project_get_policy_set(&self, project_id: &Uuid) -> Result<PolicySet, CacheError> {
        let static_policies = self.project_get_policies(project_id).await?;
        let templates = self.project_get_templates(project_id).await?;
        let template_links = self.project_get_template_links(project_id).await?;

        Ok(PolicySet {
            static_policies,
            templates,
            template_links,
        })
    }

    async fn project_set_policy_set(&self, project_id: &Uuid, policy_set: &PolicySet) -> Result<(), CacheError> {
        self.project_set_policies(project_id, &policy_set.static_policies).await?;
        self.project_set_templates(project_id, &policy_set.templates).await?;
        self.project_set_template_links(project_id, &policy_set.template_links).await?;
        Ok(())
    }
}
