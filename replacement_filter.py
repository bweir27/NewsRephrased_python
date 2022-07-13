import re
from avoidterms import IGNORE_TERMS
from constants import WORDMAP_SWAP_CASES
from wordmap import WORD_MAP


def apply_replacement_filter(text):
    lower_text = str(text)
    replaced_words_list = []
    for c in IGNORE_TERMS:
        if c in lower_text.lower():
            return {
                "num_replacements": 0,
                "original_text": f'AVOID: "{c}"',
                "modified_text": f'AVOID: "{c}"',
                "replaced_keys": replaced_words_list
            }
    num_replacements = 0
    result = lower_text
    for key in WORD_MAP:
        case_insensitive = re.compile(re.escape(key), re.IGNORECASE)
        found = re.search(case_insensitive, result)
        if found:
            # Prevent re-swapping
            if key in WORDMAP_SWAP_CASES and WORD_MAP[key] in replaced_words_list:
                continue
            else:
                result = re.sub(pattern=case_insensitive, repl=WORD_MAP[key], string=result, count=10)
                replaced_words_list.append(key)
                num_replacements += 1
    result = result.replace('  ', ' ')
    return {
        "num_replacements": num_replacements,
        "original_text": text,
        "modified_text": result,
        "replaced_keys": replaced_words_list
    }
