"""
Rule-based topic / category classifier for automotive parts.

Scoring: each matching keyword in the combined text adds +1 to that topic's
score. The highest-scoring topic wins (ties broken alphabetically). When no
keyword matches at all the result is "general_auto_parts".

Topics cover the main EPC (Electronic Parts Catalogue) subtree categories
used by Egyptian automotive aftermarket suppliers.
"""
from __future__ import annotations

import re
from typing import Dict, List, Tuple

# ---------------------------------------------------------------------------
# Topic taxonomy with English + Arabic trigger keywords
# ---------------------------------------------------------------------------

_TOPICS: Dict[str, List[str]] = {
    "engine": [
        # English
        "engine", "piston", "cylinder", "crankshaft", "camshaft", "valve",
        "timing", "gasket", "oil pump", "water pump", "head gasket", "block",
        "connecting rod", "oil seal", "cam belt", "timing belt", "timing chain",
        "head cover", "rocker arm", "tappet", "lifter", "manifold", "intake",
        "engine mount", "motor mount",
        # Arabic – general
        "محرك", "بستم", "اسطوانة", "أسطوانة", "عمود كامة", "عمود المرفق", "عمود مرفق",
        "صمام", "توقيت", "حشية", "مضخة زيت", "مضخة ماء", "سيل زيت", "حزام توقيت",
        "سلسلة توقيت", "غطاء المحرك", "ذراع رفع", "مشعب",
        # Arabic – EPC category names (from egycarparts)
        "حامل المحرك", "وسادة المحرك", "مضخات المياه", "حشيات المحرك",
        "غطاء رأس الاسطوانات", "سيور التوقيت", "سلاسل التوقيت",
        "مبردات الزيت", "أنابيب زيت", "أنابيب الزيت",
    ],
    "brakes": [
        # English
        "brake", "rotor", "caliper", "pad", "disc", "abs", "drum", "shoe",
        "master cylinder", "wheel cylinder", "brake fluid", "brake booster",
        "brake line", "brake hose", "handbrake", "parking brake", "brake sensor",
        "wear sensor", "brake disc", "brake drum",
        # Arabic – general
        "فرامل", "قرص فرامل", "قرص", "فحمة", "فحمة فرامل", "اسطوانة تثبيت",
        "سائل فرامل", "طبلة", "طبلة فرامل", "مبستر فرامل", "خرطوم فرامل",
        "فرامل يد", "حساس فرامل", "حساس تآكل",
        # Arabic – EPC category names
        "وسادات الفرامل", "اقراص الفرامل", "أقراص الفرامل", "اسطوانات الفرامل",
        "طبلات الفرامل", "أحذية الفرامل", "مجموعة الفرامل",
    ],
    "suspension_steering": [
        # English
        "shock", "shock absorber", "strut", "spring", "coil spring", "leaf spring",
        "control arm", "stabilizer", "sway bar", "ball joint", "tie rod",
        "rack", "pinion", "steering rack", "bearing", "wheel bearing", "hub",
        "spindle", "bushing", "wishbone", "knuckle", "trailing arm", "anti-roll",
        # Arabic – general
        "تعليق", "ماصة", "ماصة صدمات", "زنبرك", "ذراع", "ذراع علوي", "ذراع سفلي",
        "مثبت", "مفصل كروي", "قضيب ربط", "رمان توجيه", "بوش", "كاوتش تعليق",
        "محور عجلة", "منتصف عجلة",
        # Arabic – EPC category names
        "ممتصات الصدمات", "نوابض التعليق", "مشابك قضيب منع الانقلاب",
        "وصلات ومشابك قضيب منع الانقلاب", "قضيب منع الانقلاب",
        "مجموعة التوجيه", "نهاية قضيب التوجيه", "علب التوجيه",
    ],
    "transmission_drivetrain": [
        # English
        "transmission", "gearbox", "clutch", "flywheel", "driveshaft",
        "axle", "differential", "cv joint", "prop shaft", "gear",
        "torque converter", "gear oil", "transfer case", "output shaft",
        # Arabic – general
        "ناقل حركة", "فتيس", "علبة سرعات", "كلتش", "قرص تقاطر", "محور", "محور امامي",
        "محور خلفي", "تفاضلي", "وصلة كاردان", "عمود ادارة",
        # Arabic – EPC category names
        "مجموعة الكلتش", "قرص القابض", "تعليق المحور",
    ],
    "electrical_ignition": [
        # English
        "alternator", "starter", "battery", "spark plug", "ignition coil",
        "sensor", "ecu", "relay", "fuse", "wiring", "switch", "lamp", "light",
        "oxygen sensor", "map sensor", "throttle position", "crankshaft sensor",
        "abs sensor", "speed sensor", "temperature sensor", "pressure sensor",
        "control module", "horn", "indicator", "bulb", "generator",
        # Arabic – general
        "دينمو", "سلف", "بطارية", "شمعة اشعال", "بوجيه", "ملف اشعال", "حساس",
        "وحدة تحكم", "ريليه", "فيوز", "مصباح", "لمبة", "بوق", "مفاتيح",
        "حساس اكسجين", "حساس سرعة", "حساس حرارة",
        # Arabic – EPC category names (the big offenders)
        "شمعات الإشعال", "شمعات إشعال", "شمعات", "بواجي",
        "المولدات", "المولد", "مولدات الكهرباء",
        "بدايات التشغيل", "موتور البداية",
        "مجسات", "مجس", "حساسات",
        "أضواء", "مصابيح", "إضاءة السيارة",
        "كهرباء السيارة",
    ],
    "cooling_hvac": [
        # English
        "radiator", "thermostat", "fan", "coolant", "heater", "ac", "air conditioning",
        "condenser", "compressor", "evaporator", "hose", "radiator hose",
        "water pump", "overflow tank", "expansion tank", "intercooler",
        # Arabic – general
        "رادياتير", "ترموستات", "مروحة", "سائل تبريد", "سخان", "مكيف",
        "مكثف", "ضاغط", "تبخير", "خرطوم مياه", "خرطوم تبريد", "تانك مياه",
        # Arabic – EPC category names
        "مشعات السيارات", "المشعات", "مشعة", "مبردات",
        "خراطيم التبريد", "منظومة التبريد",
        "ضواغط مكيف الهواء", "مكثفات مكيف الهواء", "مبخرات مكيف الهواء",
        "منفاخ مكيف الهواء",
    ],
    "fuel_exhaust": [
        # English
        "fuel pump", "injector", "carburetor", "throttle", "fuel filter",
        "muffler", "exhaust", "catalytic", "converter", "oxygen sensor",
        "fuel rail", "fuel line", "intake manifold", "exhaust manifold",
        "egr", "dpf", "air filter", "fuel tank", "diesel",
        # Arabic – general
        "طلمبة بنزين", "حاقن", "كربريتر", "خانق", "فلتر بنزين", "شكمان",
        "عادم", "كاتلست", "سكر هواء", "خزان وقود", "بنزين", "ديزل",
        "مشعب عادم",
        # Arabic – EPC category names
        "مضخات الوقود", "حاقنات الوقود", "ماسورة العادم", "مجموعة العادم",
    ],
    "body_exterior": [
        # English
        "bumper", "fender", "hood", "bonnet", "door", "mirror", "headlight",
        "taillight", "grille", "panel", "window", "glass", "wiper", "seal",
        "trim", "spoiler", "bodykit", "quarter panel", "mudguard", "apron",
        # Arabic – general
        "صدام", "جناح", "كبوت", "باب", "مرآة", "مصباح امامي", "مصباح خلفي",
        "شبكة", "زجاج", "ماسحة زجاج", "مداعمة", "تشكيل خارجي",
        # Arabic – EPC category names
        "مصابيح أمامية", "مصابيح خلفية", "مصابيح القيادة",
        "فوانيس", "الزجاج الأمامي", "مساحات الزجاج",
    ],
    "interior": [
        # English
        "seat", "dashboard", "console", "carpet", "mat", "handle",
        "steering wheel", "airbag", "seatbelt", "pedal", "knob", "armrest",
        "door panel", "headliner", "sun visor", "gear knob", "cup holder",
        # Arabic – general
        "مقعد", "لوحة عدادات", "سجادة", "عجلة قيادة", "وسادة هوائية",
        "حزام امان", "واقي شمس", "تصليح داخلي", "ذراع ناقل السرعات",
        # Arabic – EPC category names
        "مجموعة دواسات",
    ],
    "wheels_tyres": [
        # English
        "wheel", "rim", "tyre", "tire", "hub cap", "lug nut", "spacer",
        "alloy wheel", "steel wheel", "wheel stud", "centre cap",
        # Arabic – general
        "عجلة", "جنط", "اطار", "إطار", "غطاء عجلة", "برغي عجلة",
        # Arabic – EPC category names and product titles
        "جنوط", "جنطة", "جنطات", "سبائك", "اطارات", "جنوط سبائك",
    ],
    "filters_fluids": [
        # English
        "oil filter", "air filter", "cabin filter", "fuel filter",
        "hydraulic", "grease", "lubricant", "fluid", "engine oil",
        "transmission fluid", "brake fluid", "power steering fluid",
        # Arabic – general
        "فلتر زيت", "فلتر هواء", "فلتر مقصورة", "فلتر مكيف", "زيت محرك",
        "زيت ناقل", "زيت فرامل", "شحم", "سوائل",
        # Arabic – EPC category names
        "مرشحات الزيت", "مرشحات الهواء", "مرشحات الوقود", "مرشحات المقصورة",
        "مرشح", "مرشحات", "فلاتر الزيت", "فلاتر الهواء",
    ],
}

# Pre-compile as single regex per topic for fast matching
_COMPILED: List[Tuple[str, re.Pattern[str]]] = []
for _topic, _keywords in _TOPICS.items():
    escaped = [re.escape(kw) for kw in _keywords]
    pattern = re.compile(r"\b(?:" + "|".join(escaped) + r")\b", re.IGNORECASE)
    _COMPILED.append((_topic, pattern))


def classify_topic(
    text: str,
    category: str = "",
    part_number: str = "",
) -> str:
    """
    Return the best-matching topic label for the product text.

    Searches across name, description, category, and part_number.
    Returns 'general_auto_parts' when no topic scores above zero.
    """
    combined = " ".join(filter(None, [text, category, part_number]))
    if not combined.strip():
        return "general_auto_parts"

    scores: Dict[str, int] = {}
    for topic, pattern in _COMPILED:
        count = len(pattern.findall(combined))
        if count:
            scores[topic] = count

    if not scores:
        return "general_auto_parts"
    return max(scores, key=lambda t: (scores[t], t))  # stable tie-break by name


def confidence_score(text: str, category: str = "") -> float:
    """Return a 0-1 confidence that the classification is reliable."""
    combined = f"{text} {category}"
    total_hits = sum(len(pat.findall(combined)) for _topic, pat in _COMPILED)
    return min(1.0, total_hits / 5)
