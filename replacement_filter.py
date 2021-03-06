import pprint
import re
from blocked_terms import BLOCKED_TERMS
from wordmap import WORD_MAP, WORDMAP_SWAP_CASES


def normalize_str(text: str) -> str:
    res = re.sub(pattern=r'\s+', repl=' ', string=text, count=20)
    res = re.sub(pattern=r'\s+([?.,:;!"](?:\s|$))', repl=r'\1', string=res, count=20)
    res = res.strip()
    return res


def apply_replacement_filter(text: str) -> dict:
    lower_text = str(text)
    replaced_key_set = set()
    replaced_key_freq_lst = list()
    for c in BLOCKED_TERMS:
        if c in lower_text.lower():
            return {
                "num_replacements": 0,
                "original_text": f'AVOID: "{c}"',
                "modified_text": f'AVOID: "{c}"',
                "replaced_key_freq": replaced_key_freq_lst
            }
    num_replacements = 0
    result = lower_text
    for key in WORD_MAP:
        case_insensitive = re.compile(re.escape(key), re.IGNORECASE)
        found = re.search(case_insensitive, result)
        if found:
            # Prevent re-swapping
            if key in WORDMAP_SWAP_CASES and WORD_MAP[key] in replaced_key_set:
                continue
            else:
                result, count = re.subn(pattern=case_insensitive, repl=WORD_MAP[key], string=result)
                replaced_key_freq_lst.append({"key": key, "freq": count})
                replaced_key_set.add(key)
                num_replacements += count
    result = normalize_str(result)
    return {
        "num_replacements": num_replacements,
        "original_text": text,
        "modified_text": result,
        "replaced_key_freq": replaced_key_freq_lst
    }
