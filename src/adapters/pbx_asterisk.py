from typing import Any, Dict


class AsteriskPbx:
    def parse_filename(self, name: str) -> Dict[str, Any]:
        return parse_filename(name)

def parse_filename(name: str) -> Dict[str, Any]:
    """Parse FreePBX filename format: dir-dst-src-YYYYMMDD-HHMMSS-uniqueid.wav"""
    base = name.rsplit(".", 1)[0] if "." in name else name
    parts = base.split("-")
    meta: Dict[str, Any] = {"raw_name": name}

    if len(parts) < 6:
        meta["direction"] = "unknown"
        return meta

    dir_tag, dst, src, yyyymmdd, hhmmss = parts[0], parts[1], parts[2], parts[3], parts[4]
    uniqueid = "-".join(parts[5:])

    direction = "incoming" if dir_tag == "in" else "outgoing" if dir_tag == "out" else "unknown"

    meta.update({
        "direction": direction,
        "dst_number": dst,
        "src_number": src,
        "date": yyyymmdd,
        "time": hhmmss,
        "asterisk_uniqueid": uniqueid,
    })
    return meta


