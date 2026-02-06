# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in databot, please report it responsibly:

1. **Do NOT** open a public GitHub issue for security vulnerabilities.
2. Report via [GitHub Security Advisories](https://github.com/asb108/databot/security/advisories) or email the maintainer at **asb108@users.noreply.github.com** with:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)
3. You will receive an acknowledgment within **48 hours**.
4. We will work with you to understand and address the issue before any public disclosure.

## Security Design Principles

databot is designed with security in mind:

- **Read-only SQL by default**: The `read_only: true` setting blocks INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, GRANT, and REVOKE statements.
- **Workspace sandboxing**: When `restrict_to_workspace: true`, filesystem and shell operations are confined to the workspace directory.
- **Command allowlist**: The `allowed_commands` list restricts which shell commands the agent can execute.
- **No stored credentials**: API keys are injected via environment variables, not stored in config files.
- **Minimal dependencies**: Small dependency tree reduces supply chain attack surface.
- **No default network exposure**: The gateway binds to `0.0.0.0` only when explicitly started.

## Scope

The following are **in scope** for security reports:

- Authentication/authorization bypasses
- Remote code execution vulnerabilities
- Path traversal beyond workspace sandbox
- SQL injection through the tool layer
- Credential leakage in logs or responses
- Dependency vulnerabilities (critical/high severity)

The following are **out of scope**:

- Prompt injection attacks (inherent to LLM-based systems)
- Denial of service via excessive API calls
- Issues requiring physical access to the host
- Social engineering attacks
