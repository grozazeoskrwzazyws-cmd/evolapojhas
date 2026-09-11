# KRO Telegram Bot — Railway

نسخة جاهزة لتشغيل بوت Telegram على Railway باستخدام Python وSQLite، مع Telegram Storage Group كمصدر دائم للاسترجاع.

## الملفات

- `kro_bot_storage.py` — الكود الرئيسي.
- `requirements.txt` — المكتبات المطلوبة.
- `railway.toml` — إعداد البناء والتشغيل على Railway.
- `.env.example` — أسماء المتغيرات المطلوبة.
- `.gitignore` — يمنع رفع قاعدة البيانات والجلسات والأسرار.

## المتغيرات المطلوبة في Railway

```text
BOT_TOKEN=توكن البوت
STORAGE_CHAT_ID=ايدي كروب التخزين
API_ID=Telegram API ID
API_HASH=Telegram API HASH
```

ضع القيم داخل Railway Variables ولا ترفع ملف `.env` أو الأسرار إلى GitHub.

## التشغيل

اربط مستودع GitHub بالمشروع في Railway. سيقرأ Railway `railway.toml` ويشغل:

```text
python kro_bot_storage.py
```

ويمكن أيضًا ضبط أمر التشغيل يدويًا إلى نفس الأمر.

## Storage Group

أضف البوت إلى مجموعة التخزين التي تستخدمها واحرص على أن يستطيع الوصول إلى الرسائل التي سيكتبها ويقرأها.

`database.db` تخزين محلي سريع وليس المصدر الدائم الوحيد. عند فقدان قاعدة SQLite، يستطيع البوت إعادة بناء بياناته من Telegram Storage Group.

## ملاحظات

- لا يتم تنزيل الوسائط إلى السيرفر؛ يتم الاعتماد على Telegram `file_id`.
- توجد آلية Queue محلية لإعادة محاولة سجلات Storage عند فشل الإرسال.
- توجد آلية Recovery لإعادة بناء SQLite من Storage Group.
- لا ترفع ملفات الجلسة أو `database.db` إلى GitHub.
