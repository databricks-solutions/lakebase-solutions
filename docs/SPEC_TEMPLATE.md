# Spec Template

_Reusable blueprint for Databricks/Lakebase projects cloned from a template (e.g. lakebase_fsm)._
_Copy this file to `SPEC_<project>.md`, fill the «brackets», delete this line._

---

## How to use this template
A good spec is **testable, bounded, and decision-recording**:
- **Testable** — every success criterion is something a skeptic could check and say "nope, not done."
- **Bounded** — the Out-of-Scope list is a feature, not an afterthought. It stops scope creep cold.
- **Decision-recording** — when we pick A over B, the spec says *why*, so we don't relitigate at hour six.

If a sentence can't be verified or doesn't constrain a decision, it's decoration — cut it.

---

# Spec: «New Project Name»
_Cloned from: «lakebase_fsm» · Author: Chase · Date: «YYYY-MM-DD»_

## 1. Problem & Goal
«What pain does this solve, for whom (customer/persona), why now? One paragraph.»
Databricks customers want to be able to run workshops across a suite of tools and services in Databricks. We want to create a reusable foundation for workshops specific to Databricks Lakebase and for Databricks Solutions Architects to use with their customers. We want to modularize the project, so that we have independent modules to deploy everything. For example, since this is all going to be lakebase oriented, the solution should always deploy lakebase. However, different workshop modules based on specific problem areas or personas might be needed or unneeded for a particular engagement. We want to structure this project is such a way where there are fundamental/core components that will always be installed and/or run since that are foundational components like lakebase, etc. Modules beyond this should include things like their own datasets, tables and catalogs, their own prebuilt genie or agent resources, security or key management, ML Models, apps, etc. Foundational components are going to include Lakebase, Service Principals needed, Key management (databricks secrets), administrative tools like the FSM Databricks app (since it contains admin controls we've built), user management, etc.

In this initial spec, we want to design the overall repo architecture to support this approach, so that modules can be easily added later. We also want this deployment to be immutable like the lakebase_fsm project is today and a single deployment notebook that is easy for a human user to interact with should be how the total deployment works. We will also want to provide instructions in our repo for how future modules, etc should be integrated into the existing codebase. We do not want to have to touch the deployment notebook on every new module either, so parameterizing that notebook will likely be paramount.

## 2. Success Criteria (acceptance tests)
- [ ] Deploys end-to-end via a single parameterized notebook
- [ ] Foundational components are always deployed
- [ ] Modules are deployed on an as needed basis
- [ ] Multiple Modules can be deployed easily at a time
- [ ] New modules integration works and instructions provided are functional
- [ ] Lakebase Data API works and is functional

## 3. Scope
**In:**
- New Lakebase Instance and its deployment and configuration
- Installation and deployment of any modules discovered in the project via an easy to use single notebook
- Security is paramount since this will be a public facing repo - security posture will be assessed and should be strong
- All major personas: DBAs, App Developers, AI Engineers, Technical Leadership, ETL Developers
- Data API for Lakebase instance that is created
- Teardown of all assets via the same deploy script to clean up everything used for the workshop after its completed

**Out (parking lot — capture later ideas here, don't build them now):**
- New module creation (short of a canary module for testing)

## 4. Constraints & Non-Negotiables
- Deploy runs INSIDE Databricks notebooks — no local CLI (Pulumi/DABs CLI are non-starters)
- Never execute code locally against Databricks/Lakebase — commit → push → pull → run from the workspace
- Standalone assets: own PG roles, secret scope, credentials — nothing reused from another app
- Immutable / repeatable deploy; no hardcoded secrets
- Single parameterized notebook deploys everything (customer params in, full stack out)
- Every ad-hoc fix lands back in the deploy scripts

## 5. Inputs & Parameters (what varies per deploy)
I'd like to work on these interactively as we move through the project and I'd like you to infer the best practice from what our goals are before this.
