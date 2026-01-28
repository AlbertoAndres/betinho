import sys
import asyncio
import random
from azure.identity import DeviceCodeCredential
from msgraph import GraphServiceClient

CLIENT_ID = "d97ec542-b4ee-4e8b-8a3f-84e8897af79c"
TENANT_ID = "2a90a1d8-d5f8-4a6c-b1da-884bd517baf6"

SCOPES = ["https://graph.microsoft.com/CrossTenantInformation.ReadBasic.All"]

# Tuning knobs
MAX_CONCURRENCY = 5
MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1.5


def _get_status_code(ex: Exception):
    """
    Best-effort extraction of HTTP status code from Graph SDK exceptions
    without relying on internal classes.
    """
    for attr in ("status_code", "status", "response_status"):
        if hasattr(ex, attr):
            return getattr(ex, attr)
    return None


async def lookup_domain(domain: str, graph_client: GraphServiceClient, semaphore: asyncio.Semaphore):
    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                tenant_info = await (
                    graph_client.tenant_relationships
                    .find_tenant_information_by_domain_name_with_domain_name(domain)
                    .get()
                )

                return {
                    "domain": domain,
                    "tenantId": tenant_info.tenant_id,
                    "displayName": tenant_info.display_name,
                    "defaultDomainName": tenant_info.default_domain_name,
                    "federationBrandName": tenant_info.federation_brand_name,
                }

            except Exception as ex:
                status = _get_status_code(ex)

                # Retry only on throttling / transient failures
                if status in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                    delay = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    delay += random.uniform(0, 0.5)  # jitter
                    await asyncio.sleep(delay)
                    continue

                return {
                    "domain": domain,
                    "error": f"{type(ex).__name__}: {ex}",
                }


async def main(domains: list[str]):
    if not domains:
        print("Usage: python tenant_info_by_domain_parallel.py <domain1> [domain2] [...]")
        return

    credential = DeviceCodeCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
    )

    graph_client = GraphServiceClient(
        credentials=credential,
        scopes=SCOPES,
    )

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    tasks = [
        lookup_domain(domain, graph_client, semaphore)
        for domain in domains
    ]

    results = await asyncio.gather(*tasks)

    for r in results:
        print(f"\nDomain: {r['domain']}")
        if "error" in r:
            print(f"  ERROR: {r['error']}")
        else:
            print("  tenantId:", r["tenantId"])
            print("  displayName:", r["displayName"])
            print("  defaultDomainName:", r["defaultDomainName"])
            print("  federationBrandName:", r["federationBrandName"])


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
