# Do I Need a Commercial License?

HuskHoard is open source under **AGPLv3**, and it always will be. Most people using it never need anything else. This page explains the handful of situations where a commercial license makes sense, either because the law requires it or because it's simply the easier path for your business.

*This page is a plain-language guide, not legal advice. If you need a definitive answer for your specific situation, talk to your own counsel — or info@huskhoard.com and we're happy to point you in the right direction.*

---

## You almost certainly do NOT need a commercial license if you:

- Run HuskHoard on your own servers or your customers' servers, for internal use
- Install it for a client as part of a hardware/support deal, where the client operates it themselves
- Use it unmodified, in any context
- Contribute changes back to the open-source project
- Are a hobbyist, researcher, or self-hoster archiving your own data

In these cases, AGPLv3 asks nothing of you beyond what any open-source license asks: if you distribute a *modified* version, share those modifications under the same license.

## You likely DO need a commercial license if you:

**Offer a modified version of HuskHoard as a hosted service to others.**
AGPLv3's defining feature — the thing that sets it apart from plain GPL — is that it also applies when you let people use a modified version over a network, even if you never technically "distribute" it. If you're an MSP, backup-as-a-service provider, or cloud vendor and you've changed the code, you need to either publish your modifications or license commercially instead.

**Embed HuskHoard inside your own product and ship it to customers.**
If you're an OEM or ISV bundling a modified version of HuskHoard into a commercial appliance or software product, that's distribution of a derivative work, and AGPL requires you to release your changes. A commercial license lets you keep your integration closed.

**Want the closed-source enterprise features.**
Some sidecar features — (work in progress) UI fleet dashboard / advanced replication policies / Multi site/ EC / RBAC / audit logs, etc. — customize to your actual roadmap — aren't released under AGPL at all. These are only available under the commercial license, regardless of how you're using the open-source core.

**Have an internal policy against AGPL, period.**
Some legal departments have a blanket "no AGPL" rule, independent of whether a specific use would actually trigger the copyleft clause. If that's your situation, we can issue a commercial license that simply sidesteps the question — no risk assessment required on your end.

**Want support, an SLA, or indemnification tied to your license.**
The commercial license can include support commitments and legal warranties that AGPL, as a license, doesn't provide by itself.

---

## What's included in the commercial license

- Relief from AGPLv3's copyleft and network-use obligations for your deployment
- Access to closed-source enterprise features (still working on those)
- Support and SLA

## Pricing

Commercial licensing is priced per node (not capacity), with volume tiers for larger deployments. Contact us for a quote.

## How to get one

Email licensing@huskhoard.com with a short description of your use case (self-hosted product, MSP/hosted service, OEM embed, etc.) and we'll get back to you with terms.

## Still not sure?

That's normal — the line between "fine under AGPL" and "needs a commercial license" depends on specifics. Email us and describe what you're building; we'd rather answer the question directly than have you guess.
