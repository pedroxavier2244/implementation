def build_dedup_key(event_type: str, metadata: dict) -> str:
    job_id = metadata.get("job_id")
    file_date = metadata.get("file_date", "unknown")
    schema_version = metadata.get("schema_version", "")

    if job_id:
        return f"job:{job_id}:{event_type}"
    if schema_version:
        return f"file_date:{file_date}:{event_type}:{schema_version}"
    return f"file_date:{file_date}:{event_type}"
