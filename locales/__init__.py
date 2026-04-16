from .ru import LANG_RU
from .kk import LANG_KK
from .en import LANG_EN
from .uk import LANG_UK
from .zh import LANG_ZH

LANGUAGES = {
    'ru': LANG_RU,
    'kk': LANG_KK,
    'en': LANG_EN,
    'uk': LANG_UK,
    'zh': LANG_ZH,
}

DEFAULT_LANGUAGE = 'ru'

def get_text(lang_code: str, key: str, **kwargs) -> str:
    """Получить текст на нужном языке"""
    lang = LANGUAGES.get(lang_code, LANGUAGES[DEFAULT_LANGUAGE])
    text = lang.get(key, key)
    
    if kwargs:
        try:
            text = text.format(**kwargs)
        except KeyError:
            pass
    
    return text
