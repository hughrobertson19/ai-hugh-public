# AI Hugh — Security Architecture Overview

**Author:** Hugh Robertson  
**Date:** April 2026

---

## 1. Data Flow Architecture

```
                         HTTPS (TLS 1.2+)
  ┌──────────┐          ┌──────────────┐          ┌─────────────────┐
  │  User     │ ──────▶  │  Web App     │ ──────▶  │  Anthropic API  │
  │ (Browser) │ ◀──────  │  (FastAPI)   │ ◀──────  │  (Claude)       │
  └──────────┘          └──────┬───────┘          └─────────────────┘
                               │
                               │ Read/Write
                               ▼
                      ┌──────────────────┐
                      │  Local Encrypted  │
                      │  Storage (JSON)   │
                      │                   │
                      │  • User profiles  │
                      │  • Chat history   │
                      │  • Deal snapshots │
                      └──────────────────┘
```

**What stays local:** User credentials, chat history, deal snapshots, all configuration files. Never transmitted externally.

**What is sent to the API:** Current chat message, user role context, timezone, and active deal summaries (business-level only). See Section 2.

---

## 2. Data Sent to the Anthropic API

Each API request contains only the information needed to generate a coaching response:

| Data Element | Example | Purpose |
|---|---|---|
| User role | "SDR, [Employer]" | Contextual framing |
| Timezone | "America/Chicago" | Time-aware scheduling logic |
| Active deal summaries | Company name, deal stage, last activity date | Pipeline coaching |
| Chat messages | Current conversation thread | Generate response |

**Anthropic API Data Handling:**

- Zero data retention. Messages are not stored after the request completes.
- Not used for model training. Anthropic's API terms explicitly exclude customer data from training.
- Not retrievable. There is no mechanism to retrieve prior API requests — data exists only in transit.
- SOC 2 Type II compliant.
- Full policy: [https://www.anthropic.com/policies/privacy](https://www.anthropic.com/policies/privacy)

**Comparison to Microsoft Copilot (the author's employer-approved AI tool):**

| | AI Hugh (Anthropic API) | Microsoft Copilot |
|---|---|---|
| Data retention | None | Retained per Microsoft 365 tenant policies |
| Training on customer data | No | No (commercial terms) |
| Data residency | US processing, no storage | Microsoft data centers per tenant config |
| Third-party subprocessors | None | Multiple (per Microsoft DPA) |
| Scope of data access | Only what is explicitly passed per request | Access to full Microsoft 365 tenant data |

AI Hugh's data exposure surface is narrower than Copilot's because data is passed explicitly per request rather than accessed broadly across a tenant.

---

## 3. Data NOT Sent to the API

The following categories of data are never included in API requests:

- Passwords or authentication credentials
- API keys or encryption keys
- Full Salesforce data exports
- Proprietary product formulations, test results, or certification details
- Customer PII beyond business contact names (no SSNs, no personal email, no financial data)
- Internal employer documents, reports, or intellectual property
- Employee records or HR data

---

## 4. Security Controls

### Authentication
- Password hashing: PBKDF2-HMAC-SHA256 with random salt
- Account lockout: 5 failed attempts triggers lockout
- No default credentials; accounts provisioned individually

### Rate Limiting
- Authentication endpoints: 5 requests/minute
- Chat endpoints: 10 requests/minute
- Prevents brute-force and abuse

### Encryption at Rest
- All stored data (user profiles, chat history, deal snapshots) encrypted using Fernet symmetric encryption (AES-128-CBC + HMAC-SHA256)
- Encryption keys are cryptographically random, generated at runtime
- No hardcoded secrets in source code

### Audit Logging
- All authentication events logged (login, logout, failed attempts, lockouts)
- Access events logged with timestamps
- Message content is not written to logs

### Security Headers
- `X-Frame-Options: DENY` — prevents clickjacking
- `X-Content-Type-Options: nosniff` — prevents MIME sniffing
- `X-XSS-Protection: 1; mode=block` — XSS mitigation
- `Referrer-Policy: strict-origin-when-cross-origin` — limits referrer leakage

---

## 5. Deployment

| Attribute | Detail |
|---|---|
| Hosting | Self-hosted or Railway (PaaS) |
| Transport | HTTPS enforced at platform level (TLS termination by Railway or reverse proxy) |
| Database | None. All data stored as encrypted local JSON files |
| Third-party integrations | None. No Salesforce API, no ZoomInfo API, no email sending, no calendar API |
| External dependencies | Anthropic API only |
| Source control | Private repository; no credentials committed |

The system has no write access to any employer production system. It is read-nothing, write-nothing with respect to corporate infrastructure. The only external call is to the Anthropic API.

---

## 6. Pilot Proposal

| Parameter | Detail |
|---|---|
| **Scope** | SDR team winback campaign — reactivating closed-lost opportunities |
| **User** | Hugh Robertson (single user for initial pilot) |
| **Duration** | 30 days |
| **Data scope** | Closed-lost opportunity data only (company name, deal stage, contact name, last activity). This data already exists in Salesforce and contains no proprietary technical information |
| **Success metrics** | Meetings booked from winback list, pipeline dollar value recovered, hours saved per week vs. manual process |
| **Access controls** | Single-user authentication; no shared accounts |
| **Exit plan** | All local data (encrypted JSON files) can be deleted with a single command. No data persists on the Anthropic side. Complete removal takes under 5 minutes |
| **Escalation path** | Any security concern escalated immediately to IT Security; system can be shut down instantly by stopping the single application process |

---

## Contact

Hugh Robertson  
Sales Development Representative  
Available for technical walkthrough or live demo upon request.
