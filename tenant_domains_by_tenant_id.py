import argparse
import asyncio
import random
from pathlib import Path

from azure.identity import DeviceCodeCredential
from msgraph import GraphServiceClient

CLIENT_ID = "<YOUR_CLIENT_ID>"
TENANT_ID = "<YOUR_TENANT_ID>"
SCOPES = ["https://graph.microsoft.com/CrossTenantInformation.ReadBasic.All"]

MAX_CONCURRENCY = 5
MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1.5


def _get_status_code(ex: Exception):
    for attr in ("status_code", "status", "response_status"):
        if hasattr(ex, attr):
            return getattr(ex, attr)
    return None


def _extract_domains(tenant_info) -> list[str]:
    domains: set[str] = set()

    for attr_name in ("default_domain_name", "defaultDomainName"):
        value = getattr(tenant_info, attr_name, None)
        if isinstance(value, str) and value.strip():
            domains.add(value.strip().lower())

    for attr_name in ("verified_domains", "verifiedDomains", "domains", "domain_names", "domainNames"):
        value = getattr(tenant_info, attr_name, None)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.strip():
                    domains.add(item.strip().lower())
                elif isinstance(item, dict):
                    candidate = item.get("name") or item.get("domain") or item.get("domainName")
                    if isinstance(candidate, str) and candidate.strip():
                        domains.add(candidate.strip().lower())
                else:
                    candidate = getattr(item, "name", None) or getattr(item, "domain", None)
                    if isinstance(candidate, str) and candidate.strip():
                        domains.add(candidate.strip().lower())

    additional = getattr(tenant_info, "additional_data", None)
    if isinstance(additional, dict):
        for key in ("verifiedDomains", "domains", "domainNames"):
            value = additional.get(key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, str) and item.strip():
                        domains.add(item.strip().lower())
                    elif isinstance(item, dict):
                        candidate = item.get("name") or item.get("domain") or item.get("domainName")
                        if isinstance(candidate, str) and candidate.strip():
                            domains.add(candidate.strip().lower())

    return sorted(domains)


async def lookup_tenant_domains(tenant_id: str, graph_client: GraphServiceClient, semaphore: asyncio.Semaphore):
    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                tenant_info = await (
                    graph_client.tenant_relationships
                    .find_tenant_information_by_tenant_id_with_tenant_id(tenant_id)
                    .get()
                )

                domains = _extract_domains(tenant_info)
                if not domains:
                    return {
                        "tenantId": tenant_id,
                        "error": "No domains found in Graph response for this tenant.",
                    }

                return {
                    "tenantId": tenant_id,
                    "domains": domains,
                }

            except Exception as ex:
                status = _get_status_code(ex)
                if status in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                    delay = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    delay += random.uniform(0, 0.5)
                    await asyncio.sleep(delay)
                    continue

                return {
                    "tenantId": tenant_id,
                    "error": f"{type(ex).__name__}: {ex}",
                }


def _load_tenant_ids(input_file: Path) -> list[str]:
    tenant_ids: list[str] = []
    with input_file.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            tenant_ids.append(line)

    deduped = list(dict.fromkeys(tenant_ids))
    return deduped


def _write_output(output_file: Path, results: list[dict]):
    lines: list[str] = []
    for result in results:
        tenant_id = result["tenantId"]
        lines.append(f"TenantId: {tenant_id}")
        if "error" in result:
            lines.append(f"ERROR: {result['error']}")
        else:
            for domain in result["domains"]:
                lines.append(domain)
        lines.append("")

    output_file.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


async def run(input_file: Path, output_file: Path):
    tenant_ids = _load_tenant_ids(input_file)
    if not tenant_ids:
        raise ValueError("Input file did not contain any tenant IDs.")

    credential = DeviceCodeCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
    )

    graph_client = GraphServiceClient(
        credentials=credential,
        scopes=SCOPES,
    )

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    tasks = [lookup_tenant_domains(tenant_id, graph_client, semaphore) for tenant_id in tenant_ids]
    results = await asyncio.gather(*tasks)
    _write_output(output_file, results)

    print(f"Processed {len(results)} tenant IDs.")
    print(f"Output written to: {output_file}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read tenant IDs from a text file and output domains for each tenant.",
    )
    parser.add_argument("input_file", type=Path, help="Path to input .txt containing one tenant ID per line")
    parser.add_argument("output_file", type=Path, help="Path to output .txt")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run(args.input_file, args.output_file))
