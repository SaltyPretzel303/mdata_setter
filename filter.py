def filter(datum: dict[str, any]) -> bool:
	return True
	return datum.get("datum_id", None) is not None
