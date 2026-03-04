from flask import Flask, render_template, request, redirect, url_for, session
import json
import os
from functools import wraps
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
import random

app = Flask(__name__)
app.secret_key = 'mss-minhsonstone-secret-key-2024'

DATA_DIR = 'data'
PRODUCTS_FILE = os.path.join(DATA_DIR, 'products.json')
COMMENTS_FILE = os.path.join(DATA_DIR, 'comments.json')
CONTACTS_FILE = os.path.join(DATA_DIR, 'contacts.json')
STATS_FILE = os.path.join(DATA_DIR, 'stats.json')
ANALYTICS_EVENTS_FILE = os.path.join(DATA_DIR, 'analytics_events.json')
# Use top-level data.json for shop info & categories
SHOP_FILE = os.path.join(os.path.dirname(__file__), 'data.json')

VALID_PASSCODES = ['vananh01923', 'vananh2805', '280511']
UPLOAD_DIR = os.path.join('static', 'uploads')
PLACEHOLDER_IMAGE = '/static/images/placeholder.png'
ANALYTICS_EVENT_TYPES = {
    'view': 'views',
    'like': 'likes',
    'dislike': 'dislikes',
    'favorite_add': 'favorites',
    'comment': 'comments',
}

def load_json(p):
    try:
        with open(p, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return []

def save_json(p, d):
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

def _to_int(value, default=0):
    try:
        return int(value)
    except:
        return default

def _parse_vn_datetime(value):
    raw = str(value or '').strip()
    if not raw:
        return None
    patterns = (
        '%d/%m/%Y %H:%M',
        '%d/%m/%Y',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
    )
    for fmt in patterns:
        try:
            return datetime.strptime(raw, fmt)
        except:
            continue
    return None

def _parse_ymd_date(value):
    raw = str(value or '').strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, '%Y-%m-%d').date()
    except:
        return None

def _to_day_key(value=None):
    if value is None:
        return datetime.now().date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    try:
        return value.isoformat()
    except:
        return datetime.now().date().isoformat()

def _date_iter(start_day, end_day):
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)

def _format_day_label(day_key):
    try:
        return datetime.strptime(day_key, '%Y-%m-%d').strftime('%d/%m/%Y')
    except:
        return day_key

def _parse_date_range(from_date_raw=None, to_date_raw=None, period_raw=None):
    today = datetime.now().date()
    period = str(period_raw or 'week').strip().lower()
    from_day = _parse_ymd_date(from_date_raw)
    to_day = _parse_ymd_date(to_date_raw)

    # Explicit date range has priority and is treated as custom.
    if from_day or to_day:
        if not from_day:
            from_day = to_day or today
        if not to_day:
            to_day = from_day
        if from_day > to_day:
            from_day, to_day = to_day, from_day
        return from_day, to_day, 'custom'

    if period == 'month':
        return today.replace(day=1), today, 'month'
    if period == 'year':
        return today.replace(month=1, day=1), today, 'year'
    if period == 'all':
        return None, None, 'all'

    # Default to the last 7 days (including today).
    return today - timedelta(days=6), today, 'week'

def _load_analytics_events():
    rows = load_json(ANALYTICS_EVENTS_FILE)
    if not isinstance(rows, list):
        return []
    events = []
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        product_id = _to_int(row.get('product_id'), 0)
        event_type = str(row.get('event_type') or '').strip().lower()
        count = _to_int(row.get('count'), 0)
        if product_id <= 0 or event_type not in ANALYTICS_EVENT_TYPES:
            continue
        if count <= 0:
            count = 1
        day_key = str(row.get('day') or '').strip()
        if not day_key:
            ts_raw = str(row.get('ts') or '').strip()
            parsed_ts = None
            if ts_raw:
                try:
                    parsed_ts = datetime.fromisoformat(ts_raw)
                except:
                    parsed_ts = None
            if parsed_ts:
                day_key = parsed_ts.date().isoformat()
            else:
                parsed = _parse_vn_datetime(row.get('date'))
                day_key = _to_day_key(parsed if parsed else None)
        ts = str(row.get('ts') or '').strip() or datetime.now().isoformat(timespec='seconds')
        event = {
            'id': _to_int(row.get('id'), idx),
            'ts': ts,
            'day': day_key,
            'product_id': product_id,
            'event_type': event_type,
            'count': count,
            'source': str(row.get('source') or 'runtime').strip() or 'runtime',
        }
        if isinstance(row.get('extra'), dict) and row.get('extra'):
            event['extra'] = row['extra']
        events.append(event)
    return events

def _save_analytics_events(events):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(ANALYTICS_EVENTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(events, f, ensure_ascii=False, indent=2)

def _log_analytics_event(product_id, event_type, count=1, source='runtime', extra=None):
    product_id = _to_int(product_id, 0)
    event_type = str(event_type or '').strip().lower()
    count = _to_int(count, 0)
    if product_id <= 0 or event_type not in ANALYTICS_EVENT_TYPES or count <= 0:
        return
    events = _load_analytics_events()
    next_id = max([_to_int(e.get('id'), 0) for e in events] + [0]) + 1
    now = datetime.now()
    event = {
        'id': next_id,
        'ts': now.isoformat(timespec='seconds'),
        'day': now.date().isoformat(),
        'product_id': product_id,
        'event_type': event_type,
        'count': count,
        'source': str(source or 'runtime').strip() or 'runtime',
    }
    if isinstance(extra, dict) and extra:
        event['extra'] = extra
    events.append(event)
    _save_analytics_events(events)

def _ensure_analytics_backfill(products, comments):
    events = _load_analytics_events()
    if events:
        return events

    today_key = _to_day_key()
    seeded = []
    next_id = 1

    def push_event(product_id, event_type, count):
        nonlocal next_id
        count = _to_int(count, 0)
        if _to_int(product_id, 0) <= 0 or count <= 0:
            return
        seeded.append({
            'id': next_id,
            'ts': datetime.now().isoformat(timespec='seconds'),
            'day': today_key,
            'product_id': _to_int(product_id),
            'event_type': event_type,
            'count': count,
            'source': 'backfill',
        })
        next_id += 1

    comment_counts = {}
    for comment in comments or []:
        product_id = _to_int(comment.get('product_id'), 0)
        if product_id <= 0:
            continue
        comment_counts[product_id] = comment_counts.get(product_id, 0) + 1

    for product in products or []:
        product_id = _to_int(product.get('id'), 0)
        if product_id <= 0:
            continue
        push_event(product_id, 'view', product.get('views', 0))
        push_event(product_id, 'like', product.get('likes', 0))
        push_event(product_id, 'dislike', product.get('dislikes', 0))
        push_event(product_id, 'favorite_add', product.get('love', 0))
        push_event(product_id, 'comment', comment_counts.get(product_id, 0))

    _save_analytics_events(seeded)
    return seeded

def load_shop_data():
    """Load whole data.json (dict). Ensure keys exist."""
    d = {}
    try:
        with open(SHOP_FILE, 'r', encoding='utf-8') as f:
            d = json.load(f) or {}
    except:
        d = {}
    # normalize structure
    if not isinstance(d, dict):
        d = {}
    d.setdefault('shop', d.get('shop', {}))
    d.setdefault('cards', d.get('cards', []))
    d.setdefault('categories', d.get('categories', []))
    d.setdefault('stats', d.get('stats', {}))
    return d

def save_shop_data(d):
    # ensure file dir exists
    dirn = os.path.dirname(SHOP_FILE) or '.'
    os.makedirs(dirn, exist_ok=True)
    with open(SHOP_FILE, 'w', encoding='utf-8') as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

def _is_external_url(value):
    raw = str(value or '').strip().lower()
    return raw.startswith('http://') or raw.startswith('https://')

def _normalize_upload_name(value):
    """Normalize product image value to a filename when it points to static/uploads."""
    raw = str(value or '').strip().replace('\\', '/')
    if not raw:
        return ''
    if _is_external_url(raw):
        return raw

    lowered = raw.lower()
    if '/static/uploads/' in lowered:
        marker = lowered.index('/static/uploads/') + len('/static/uploads/')
        raw = raw[marker:]
    elif lowered.startswith('static/uploads/'):
        raw = raw[len('static/uploads/'):]
    elif lowered.startswith('/uploads/'):
        raw = raw[len('/uploads/'):]
    elif lowered.startswith('uploads/'):
        raw = raw[len('uploads/'):]
    elif raw.startswith('/'):
        # keep basename for absolute paths outside static/uploads
        raw = os.path.basename(raw)

    raw = raw.split('?', 1)[0].split('#', 1)[0]
    return os.path.basename(raw.strip())

def _build_product_image_src(value):
    raw = str(value or '').strip().replace('\\', '/')
    if not raw:
        return PLACEHOLDER_IMAGE
    if _is_external_url(raw):
        return raw
    if raw.startswith('/'):
        return raw

    normalized = _normalize_upload_name(raw)
    if not normalized:
        return PLACEHOLDER_IMAGE
    if _is_external_url(normalized):
        return normalized
    return '/static/uploads/' + normalized

def _build_unique_upload_filename(original_name):
    safe_name = secure_filename(original_name or '')
    _, ext = os.path.splitext(safe_name)
    if not ext:
        ext = '.jpg'
    ext = ext.lower()
    stamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    suffix = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=6))
    return f'product_{stamp}_{suffix}{ext}'

def _save_product_image(file_storage):
    if not file_storage or not file_storage.filename:
        return ''
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    filename = _build_unique_upload_filename(file_storage.filename)
    file_storage.save(os.path.join(UPLOAD_DIR, filename))
    return filename

def _delete_upload_file_if_unused(filename, ignore_product_id=None):
    normalized = _normalize_upload_name(filename)
    if not normalized or _is_external_url(normalized):
        return

    products = load_json(PRODUCTS_FILE) or []
    for product in products:
        try:
            if ignore_product_id is not None and int(product.get('id', 0)) == int(ignore_product_id):
                continue
        except:
            pass
        image_name = _normalize_upload_name(product.get('image') or '')
        if image_name == normalized:
            return

    shop_logo = _normalize_upload_name((load_shop_data().get('shop') or {}).get('logo') or '')
    if shop_logo == normalized:
        return

    file_path = os.path.join(UPLOAD_DIR, normalized)
    if os.path.isfile(file_path):
        try:
            os.remove(file_path)
        except:
            pass

def load_shop():
    """Return shop info dict and categories/cards for templates."""
    data = load_shop_data()
    shop_raw = data.get('shop') or {}
    # keep existing normalization used elsewhere
    name = shop_raw.get('shop_name') or shop_raw.get('name') or shop_raw.get('shop') or 'MSS MinhSonStone'
    info = {
        'shop_name': name,
        'name': name,
        'banner_name': shop_raw.get('banner_name') or shop_raw.get('banner') or '',
        'location': shop_raw.get('location') or '',
        'address': shop_raw.get('address') or shop_raw.get('addr') or '',
        'phone': shop_raw.get('phone') or shop_raw.get('phone_number') or '',
        'gmail': shop_raw.get('gmail') or shop_raw.get('email') or '',
        'facebook': shop_raw.get('facebook') or '',
        'zalo': shop_raw.get('zalo') or '',
        'description': shop_raw.get('description') or '',
        'logo': shop_raw.get('logo') or ''
    }
    return {'info': info, 'cards': data.get('cards', []), 'categories': data.get('categories', [])}

def admin_required(f):
    @wraps(f)
    def w(*a, **k):
        if not session.get('admin_logged_in'):
            return redirect(url_for('admin_login'))
        return f(*a, **k)
    return w

@app.context_processor
def inject_site_context():
    # Shared shop/category data for public templates (footer/contact/etc.)
    shop_data = load_shop()
    return {
        'site_shop': shop_data.get('info', {}),
        'site_categories': shop_data.get('categories', []) or [],
    }

def _normalize_product_for_public(p):
    """Return a shallow copy of product p with safe defaults and image URL for public pages.
    Also resolve category name/slug from shop data.json so categories stay synchronized.
    For advertising mode we hide price for public pages.
    """
    prod = p.copy()
    prod.setdefault('sku', '')
    prod.setdefault('size', '')
    prod.setdefault('origin', '')
    prod.setdefault('likes', 0)
    prod.setdefault('dislikes', 0)
    prod.setdefault('visible', True)  # ensure flag exists

    # resolve category / category_slug using categories from data.json
    cats = load_shop_data().get('categories', []) or []
    candidate_slug = (prod.get('category_slug') or (prod.get('category') or '')).strip()
    if candidate_slug and not candidate_slug.islower():
        candidate_slug = candidate_slug.lower().replace(' ', '-')
    matched = None
    for c in cats:
        try:
            if str(c.get('slug','')) == str(candidate_slug) or str(c.get('name','')) == str(prod.get('category','')):
                matched = c
                break
        except:
            continue
    if matched:
        prod['category_slug'] = matched.get('slug')
        prod['category'] = matched.get('name')
    else:
        prod.setdefault('category_slug', candidate_slug)
        prod.setdefault('category', prod.get('category') or (prod.get('category_slug') or 'Khác'))

    # hide price for public advertising pages
    prod['price'] = ''

    prod['image'] = _build_product_image_src(prod.get('image') or '')
    return prod

def _products_for_public():
    prods = load_json(PRODUCTS_FILE) or []
    # sort by seq (newest first) if present, fallback to existing order
    try:
        prods = sorted(prods, key=lambda p: int(p.get('seq', 0)), reverse=True)
    except:
        pass
    # only include visible products for public pages
    visible_prods = [p for p in prods if p.get('visible', True)]
    return [_normalize_product_for_public(p) for p in visible_prods]

# new helper: simple pagination
def _paginate_list(items, page, per_page):
    try:
        page = int(page)
    except:
        page = 1
    per_page = int(per_page)
    total = len(items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    if page < 1: page = 1
    if page > total_pages: page = total_pages
    start = (page - 1) * per_page
    end = start + per_page
    return items[start:end], total, total_pages, page

@app.route('/')
def home():
    products = _products_for_public()
    featured = products[:4]
    shop = load_shop()['info']
    categories = load_shop().get('categories', []) or []
    return render_template('home.html', featured_products=featured, shop=shop, categories=categories)

@app.route('/products')
def products():
    products = _products_for_public()
    category = request.args.get('category')
    q = request.args.get('q')
    if category:
        products = [p for p in products if p.get('category_slug') == category]
    if q:
        products = [p for p in products if q.lower() in p.get('name', '').lower()]

    # pagination: 18 products per page
    page = request.args.get('page', 1)
    per_page = 18
    page_items, total, total_pages, current_page = _paginate_list(products, page, per_page)

    shop = load_shop()['info']
    categories = load_shop().get('categories', []) or []
    return render_template('products.html',
                           products=page_items,
                           shop=shop,
                           categories=categories,
                           pagination={
                               'total': total,
                               'per_page': per_page,
                               'total_pages': total_pages,
                               'current_page': current_page
                           })

@app.route('/product/<int:id>')
def product_detail(id):
    # load raw products to find product and comments, but present normalized product to template
    raw_products = load_json(PRODUCTS_FILE)
    raw_product = next((p for p in raw_products if p.get('id') == id), None)
    if not raw_product:
        return redirect(url_for('products'))
    # if product hidden, don't show on public detail
    if not raw_product.get('visible', True):
        return redirect(url_for('products'))
    # increment views in raw data
    raw_product['views'] = raw_product.get('views', 0) + 1
    save_json(PRODUCTS_FILE, raw_products)
    _log_analytics_event(id, 'view')
    # prepare normalized product for rendering
    product = _normalize_product_for_public(raw_product)
    comments = load_json(COMMENTS_FILE)
    product_comments = [c for c in comments if c.get('product_id') == id]
    related_raw = [p for p in raw_products if p.get('id') != id and p.get('visible', True)][:4]
    related = [_normalize_product_for_public(p) for p in related_raw]
    shop = load_shop()['info']
    categories = load_shop().get('categories', []) or []
    return render_template('product_detail.html', product=product, comments=product_comments, related_products=related, shop=shop, categories=categories)

# New: submit rating (endpoint name submit_rating matches template)
@app.route('/submit_rating/<int:id>', methods=['POST'])
def submit_rating(id):
    try:
        rating = int(request.form.get('rating') or 0)
    except:
        rating = 0
    products = load_json(PRODUCTS_FILE)
    prod = next((p for p in products if p.get('id') == id), None)
    if not prod:
        return redirect(url_for('products'))
    # simple heuristic: rating >=4 => like, else dislike
    if rating >= 4:
        prod['likes'] = prod.get('likes', 0) + 1
        _log_analytics_event(id, 'like')
    else:
        prod['dislikes'] = prod.get('dislikes', 0) + 1
        _log_analytics_event(id, 'dislike')

    # Keep comment-in-rating optional; only save real text to avoid empty spam rows.
    comment_text = (request.form.get('comment') or '').strip()
    if comment_text:
        comments = load_json(COMMENTS_FILE)
        comments.append({
            'id': max([c.get('id', 0) for c in comments] + [0]) + 1,
            'product_id': id,
            'product_name': prod.get('name'),
            'name': request.form.get('name') or 'Khách',
            'email': request.form.get('email') or '',
            'content': comment_text,
            'rating': rating,
            'date': datetime.now().strftime('%d/%m/%Y %H:%M'),
            'status': 'approved'
        })
        save_json(COMMENTS_FILE, comments)
        _log_analytics_event(id, 'comment', extra={'origin': 'rating'})

    save_json(PRODUCTS_FILE, products)
    return redirect(url_for('product_detail', id=id))

# New: submit comment (endpoint name submit_comment matches template)
@app.route('/submit_comment/<int:id>', methods=['POST'])
def submit_comment(id):
    comments = load_json(COMMENTS_FILE)
    products = load_json(PRODUCTS_FILE)
    prod = next((p for p in products if p.get('id') == id), None)
    comments.append({
        'id': max([c.get('id', 0) for c in comments] + [0]) + 1,
        'product_id': id,
        'product_name': prod.get('name') if prod else '',
        'name': request.form.get('name') or 'Khách',
        'email': request.form.get('email') or '',
        'content': request.form.get('content') or '',
        'date': datetime.now().strftime('%d/%m/%Y %H:%M'),
        'status': 'pending'
    })
    save_json(COMMENTS_FILE, comments)
    _log_analytics_event(id, 'comment')
    return redirect(url_for('product_detail', id=id))

@app.route('/contact')
def contact():
    return render_template('contact.html')

@app.route('/submit-contact', methods=['POST'])
def submit_contact():
    contacts = load_json(CONTACTS_FILE)
    contacts.append({
        'id': len(contacts) + 1,
        'name': request.form.get('name'),
        'phone': request.form.get('phone'),
        'email': request.form.get('email'),
        'subject': request.form.get('subject'),
        'message': request.form.get('message'),
        'date': datetime.now().strftime('%d/%m/%Y %H:%M'),
        'status': 'unread'
    })
    save_json(CONTACTS_FILE, contacts)
    return redirect(url_for('contact'))

@app.route('/admin', methods=['GET', 'POST'])
def admin_login():
    if session.get('admin_logged_in'):
        return redirect(url_for('admin_dashboard'))
    if request.method == 'POST':
        if request.form.get('passcode') in VALID_PASSCODES:
            session['admin_logged_in'] = True
            return redirect(url_for('admin_dashboard'))
        return render_template('admin/login.html', error='Sai mã đăng nhập')
    return render_template('admin/login.html')

@app.route('/admin/logout')
def admin_logout():
    session.pop('admin_logged_in', None)
    return redirect(url_for('admin_login'))

# Favorites: show/add/remove (session-based)
@app.route('/favorites')
def favorites():
    fav_ids = session.get('favorites', [])
    products = load_json(PRODUCTS_FILE)
    fav_products = [_normalize_product_for_public(p) for p in products if p.get('id') in fav_ids]
    shop = load_shop()['info']
    categories = load_shop().get('categories', []) or []
    return render_template('favorites.html', products=fav_products, shop=shop, categories=categories)

@app.route('/favorites/add/<int:id>', methods=['POST'], endpoint='favorites_add')
def add_favorite(id):
    session.setdefault('favorites', [])
    if id not in session['favorites']:
        session['favorites'].append(id)
        session.modified = True
        _log_analytics_event(id, 'favorite_add')
    return redirect(request.referrer or url_for('products'))

@app.route('/favorites/remove/<int:id>', methods=['POST'], endpoint='favorites_remove')
def remove_favorite(id):
    session.setdefault('favorites', [])
    if id in session['favorites']:
        session['favorites'].remove(id)
        session.modified = True
    return redirect(request.referrer or url_for('favorites'))

# Admin dashboard
@app.route('/admin/dashboard')
@admin_required
def admin_dashboard():
    return render_template(
        'admin/dashboard.html',
        stats=load_json(STATS_FILE),
        top_products=load_json(PRODUCTS_FILE)[:5],
        recent_comments=load_json(COMMENTS_FILE)[-5:],
        recent_contacts=load_json(CONTACTS_FILE)[-5:],
        active_page='dashboard'
    )

# Admin products pages and actions
@app.route('/admin/products')
@admin_required
def admin_products():
    # include categories from shop data so template can render dynamic selects and icons
    shop = load_shop()
    categories = shop.get('categories', []) or []
    products = load_json(PRODUCTS_FILE) or []
    try:
        products = sorted(products, key=lambda p: int(p.get('seq', 0)), reverse=True)
    except:
        pass
    # Normalize image values for robust rendering in admin template.
    for product in products:
        image_name = _normalize_upload_name(product.get('image') or '')
        product['image_name'] = '' if _is_external_url(image_name) else image_name
        product['image_src'] = _build_product_image_src(product.get('image') or '')

    return render_template('admin/products.html',
                           products=products,
                           categories=categories,
                           active_page='products')

@app.route('/admin/products/add', methods=['POST'], endpoint='admin_add_product')
@admin_required
def admin_add_product():
    products = load_json(PRODUCTS_FILE)
    name = (request.form.get('name') or '').strip()
    price_raw = (request.form.get('price') or '0').strip()
    try:
        price = float(price_raw)
    except:
        price = price_raw
    category_input = (request.form.get('category') or '').strip()
    # resolve category name & slug from data.json categories
    shop_data = load_shop_data()
    cats = shop_data.get('categories', []) or []
    matched = next((c for c in cats if str(c.get('slug','')) == category_input or str(c.get('name','')) == category_input), None)
    if matched:
        category = matched.get('name')
        category_slug = matched.get('slug')
    else:
        category_slug = category_input.lower().replace(' ', '-') if category_input else ''
        category = category_input or ''
    sku = (request.form.get('sku') or '').strip()
    size = (request.form.get('size') or '').strip()
    origin = (request.form.get('origin') or '').strip()
    description = request.form.get('description') or ''
    image_filename = _save_product_image(request.files.get('image'))

    # generate unique random 9-digit id for product
    existing_ids = {int(p.get('id')) for p in products if str(p.get('id')).isdigit()}
    def gen_prod_id():
        for _ in range(200):
            val = random.randint(100000000, 999999999)
            if val not in existing_ids:
                return val
        # fallback to sequential if unlucky
        return max(list(existing_ids)+[100000000]) + 1
    new_id = gen_prod_id()
    # maintain a sequential 'seq' for ordering (max seq + 1)
    existing_seqs = [int(p.get('seq', 0)) for p in products if str(p.get('seq', '')).isdigit()]
    new_seq = max(existing_seqs + [0]) + 1
    product = {
        'id': new_id,
        'name': name,
        'sku': sku,
        'price': price,
        'category': category,
        'category_slug': category_slug,
        'size': size,
        'origin': origin,
        'description': description,
        'image': image_filename,
        'views': 0,
        'created_at': datetime.now().strftime('%d/%m/%Y %H:%M'),
        'visible': True,
        'seq': new_seq
    }
    products.append(product)
    save_json(PRODUCTS_FILE, products)
    return redirect(url_for('admin_products'))

@app.route('/admin/products/edit/<int:id>', methods=['POST'], endpoint='admin_edit_product')
@admin_required
def admin_edit_product(id):
    products = load_json(PRODUCTS_FILE)
    prod = next((p for p in products if p.get('id') == id), None)
    if not prod:
        return redirect(url_for('admin_products'))
    old_image = _normalize_upload_name(prod.get('image') or '')
    prod['name'] = (request.form.get('name') or prod.get('name', '')).strip()
    prod['sku'] = (request.form.get('sku') or prod.get('sku', '')).strip()
    try:
        prod['price'] = float((request.form.get('price') or str(prod.get('price', 0))).strip())
    except:
        pass
    category_input = (request.form.get('category') or prod.get('category', '')).strip()
    shop_data = load_shop_data()
    cats = shop_data.get('categories', []) or []
    matched = next((c for c in cats if str(c.get('slug','')) == category_input or str(c.get('name','')) == category_input), None)
    if matched:
        prod['category'] = matched.get('name')
        prod['category_slug'] = matched.get('slug')
    else:
        prod['category_slug'] = category_input.lower().replace(' ', '-') if category_input else prod.get('category_slug', '')
        prod['category'] = category_input or prod.get('category', '')
    prod['size'] = (request.form.get('size') or prod.get('size', '')).strip()
    prod['origin'] = (request.form.get('origin') or prod.get('origin', '')).strip()
    prod['description'] = request.form.get('description') or prod.get('description', '')
    remove_image = str(request.form.get('remove_image') or '').lower() in ('1', 'true', 'yes', 'on')
    img = request.files.get('image')
    if img and img.filename:
        new_image = _save_product_image(img)
        if new_image:
            prod['image'] = new_image
            _delete_upload_file_if_unused(old_image, ignore_product_id=id)
    elif remove_image:
        prod['image'] = ''
        _delete_upload_file_if_unused(old_image, ignore_product_id=id)
    else:
        prod['image'] = old_image
    save_json(PRODUCTS_FILE, products)
    return redirect(url_for('admin_products'))

# Added: handle edit via POST with id in form (avoid BuildError when template doesn't provide id in url_for)
@app.route('/admin/products/edit', methods=['POST'])
@admin_required
def admin_edit_product_noid():
    products = load_json(PRODUCTS_FILE)
    try:
        id = int(request.form.get('id') or 0)
    except:
        return redirect(url_for('admin_products'))
    prod = next((p for p in products if p.get('id') == id), None)
    if not prod:
        return redirect(url_for('admin_products'))
    old_image = _normalize_upload_name(prod.get('image') or '')
    prod['name'] = (request.form.get('name') or prod.get('name', '')).strip()
    prod['sku'] = (request.form.get('sku') or prod.get('sku', '')).strip()
    try:
        prod['price'] = float((request.form.get('price') or str(prod.get('price', 0))).strip())
    except:
        pass
    category_input = (request.form.get('category') or prod.get('category', '')).strip()
    shop_data = load_shop_data()
    cats = shop_data.get('categories', []) or []
    matched = next((c for c in cats if str(c.get('slug','')) == category_input or str(c.get('name','')) == category_input), None)
    if matched:
        prod['category'] = matched.get('name')
        prod['category_slug'] = matched.get('slug')
    else:
        prod['category_slug'] = category_input.lower().replace(' ', '-') if category_input else prod.get('category_slug', '')
        prod['category'] = category_input or prod.get('category', '')
    prod['size'] = (request.form.get('size') or prod.get('size', '')).strip()
    prod['origin'] = (request.form.get('origin') or prod.get('origin', '')).strip()
    prod['description'] = request.form.get('description') or prod.get('description', '')
    remove_image = str(request.form.get('remove_image') or '').lower() in ('1', 'true', 'yes', 'on')
    img = request.files.get('image')
    if img and img.filename:
        new_image = _save_product_image(img)
        if new_image:
            prod['image'] = new_image
            _delete_upload_file_if_unused(old_image, ignore_product_id=id)
    elif remove_image:
        prod['image'] = ''
        _delete_upload_file_if_unused(old_image, ignore_product_id=id)
    else:
        prod['image'] = old_image
    save_json(PRODUCTS_FILE, products)
    return redirect(url_for('admin_products'))

# Added: handle delete via POST with id in form (avoid BuildError)
@app.route('/admin/products/delete', methods=['POST'])
@admin_required
def admin_delete_product_noid():
    products = load_json(PRODUCTS_FILE)
    try:
        id = int(request.form.get('id') or 0)
    except:
        return redirect(url_for('admin_products'))
    removed = next((p for p in products if p.get('id') == id), None)
    products = [p for p in products if p.get('id') != id]
    save_json(PRODUCTS_FILE, products)
    if removed:
        _delete_upload_file_if_unused(removed.get('image') or '')
    return redirect(url_for('admin_products'))

# New: toggle product visible via POST (AJAX-friendly or redirect)
@app.route('/admin/products/toggle', methods=['POST'], endpoint='admin_toggle_product')
@admin_required
def admin_toggle_product():
    try:
        # support form or JSON body
        cid = None
        if request.form.get('id'):
            cid = int(request.form.get('id'))
        else:
            js = request.get_json(silent=True) or {}
            cid = int(js.get('id') or 0)
    except:
        return ('', 400)
    products = load_json(PRODUCTS_FILE)
    for p in products:
        try:
            if int(p.get('id', 0)) == cid:
                p['visible'] = not bool(p.get('visible', True))
                new_state = p['visible']
                save_json(PRODUCTS_FILE, products)
                # AJAX JSON response
                if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
                    return {'success': True, 'visible': new_state}
                # fallback: redirect back to admin products
                return redirect(url_for('admin_products'))
        except:
            continue
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
        return {'success': False, 'message': 'Not found'}
    return redirect(url_for('admin_products'))

# Admin comments
@app.route('/admin/comments')
@admin_required
def admin_comments():
    comments = load_json(COMMENTS_FILE) or []
    q = (request.args.get('q') or '').strip()
    q_lower = q.lower()
    status = (request.args.get('status') or 'all').strip().lower()
    if status not in ('all', 'pending', 'approved', 'read'):
        status = 'all'

    filtered = []
    for raw in comments:
        if not isinstance(raw, dict):
            continue
        comment = raw.copy()
        comment_status = str(comment.get('status') or 'pending').strip().lower() or 'pending'
        comment['status'] = comment_status

        if status != 'all' and comment_status != status:
            continue

        if q_lower:
            haystack = ' '.join([
                str(comment.get('name') or ''),
                str(comment.get('email') or ''),
                str(comment.get('content') or ''),
                str(comment.get('product_name') or ''),
            ]).lower()
            if q_lower not in haystack:
                continue

        name = str(comment.get('name') or '').strip() or 'Khách'
        comment['name'] = name
        comment['email'] = str(comment.get('email') or '').strip()
        comment['content'] = str(comment.get('content') or '').strip()
        comment['product_name'] = str(comment.get('product_name') or '').strip() or 'Không xác định'
        comment['avatar'] = name[0].upper() if name else '?'
        comment['product_id'] = _to_int(comment.get('product_id'), 0)
        comment['id'] = _to_int(comment.get('id'), 0)
        comment['date'] = str(comment.get('date') or '').strip() or 'Không rõ thời gian'
        filtered.append(comment)

    def sort_key(comment):
        parsed = _parse_vn_datetime(comment.get('date'))
        return (
            parsed if parsed else datetime.min,
            _to_int(comment.get('id'), 0),
        )

    filtered.sort(key=sort_key, reverse=True)
    return render_template(
        'admin/comments.html',
        comments=filtered,
        filters={'q': q, 'status': status},
        active_page='comments'
    )

# rename existing delete-by-url-id endpoint to avoid endpoint name conflict
@app.route('/admin/comments/delete/<int:id>', methods=['POST'], endpoint='admin_delete_comment_with_id')
@admin_required
def admin_delete_comment(id):
    comments = load_json(COMMENTS_FILE)
    comments = [c for c in comments if c.get('id') != id]
    save_json(COMMENTS_FILE, comments)
    return redirect(url_for('admin_comments'))

# keep existing mark-read-by-id endpoint
@app.route('/admin/comments/mark_read/<int:id>', methods=['POST'], endpoint='admin_mark_comment_read')
@admin_required
def admin_mark_comment_read(id):
    comments = load_json(COMMENTS_FILE)
    for c in comments:
        if _to_int(c.get('id'), 0) == _to_int(id, -1):
            c['status'] = 'read'
    save_json(COMMENTS_FILE, comments)
    return redirect(url_for('admin_comments'))

# New: edit comment via POST (id in form)
@app.route('/admin/comments/edit', methods=['POST'], endpoint='admin_edit_comment')
@admin_required
def admin_edit_comment_noid():
    comments = load_json(COMMENTS_FILE)
    cid = _to_int(request.form.get('id'), 0)
    if cid <= 0:
        return redirect(url_for('admin_comments'))
    edited = False
    new_content = (request.form.get('content') or '').strip()
    new_name = (request.form.get('name') or '').strip()
    for c in comments:
        if _to_int(c.get('id'), 0) == cid:
            if new_content:
                c['content'] = new_content
            if new_name:
                c['name'] = new_name
            c['date'] = c.get('date') or datetime.now().strftime('%d/%m/%Y %H:%M')
            edited = True
            break
    if edited:
        save_json(COMMENTS_FILE, comments)
    return redirect(url_for('admin_comments'))

# New: approve comment via POST (id in form)
@app.route('/admin/comments/approve', methods=['POST'], endpoint='admin_approve_comment')
@admin_required
def admin_approve_comment_noid():
    comments = load_json(COMMENTS_FILE)
    cid = _to_int(request.form.get('id'), 0)
    if cid <= 0:
        return redirect(url_for('admin_comments'))
    for c in comments:
        if _to_int(c.get('id'), 0) == cid:
            c['status'] = 'approved'
            break
    save_json(COMMENTS_FILE, comments)
    return redirect(url_for('admin_comments'))

# New: delete comment via POST with id in form (template uses this)
@app.route('/admin/comments/delete', methods=['POST'], endpoint='admin_delete_comment')
@admin_required
def admin_delete_comment_noid():
    comments = load_json(COMMENTS_FILE)
    cid = _to_int(request.form.get('id'), 0)
    if cid <= 0:
        return redirect(url_for('admin_comments'))
    comments = [c for c in comments if _to_int(c.get('id'), 0) != cid]
    save_json(COMMENTS_FILE, comments)
    return redirect(url_for('admin_comments'))

# Admin contacts and actions
@app.route('/admin/contacts')
@admin_required
def admin_contacts():
    return render_template('admin/contacts.html', contacts=load_json(CONTACTS_FILE), active_page='contacts')

# rename existing delete-by-url-id endpoint to avoid endpoint name conflict
@app.route('/admin/contacts/delete/<int:id>', methods=['POST'], endpoint='admin_delete_contact_with_id')
@admin_required
def admin_delete_contact(id):
    contacts = load_json(CONTACTS_FILE)
    contacts = [c for c in contacts if c.get('id') != id]
    save_json(CONTACTS_FILE, contacts)
    return redirect(url_for('admin_contacts'))

# New: delete contact via POST with id in form (template uses this)
@app.route('/admin/contacts/delete', methods=['POST'], endpoint='admin_delete_contact')
@admin_required
def admin_delete_contact_noid():
    contacts = load_json(CONTACTS_FILE)
    try:
        cid = int(request.form.get('id') or 0)
    except:
        return redirect(url_for('admin_contacts'))
    contacts = [c for c in contacts if c.get('id') != cid]
    save_json(CONTACTS_FILE, contacts)
    return redirect(url_for('admin_contacts'))

# New: mark contact as read via POST (template calls admin_mark_read)
@app.route('/admin/contacts/mark_read', methods=['POST'], endpoint='admin_mark_read')
@admin_required
def admin_mark_read():
    contacts = load_json(CONTACTS_FILE)
    try:
        cid = int(request.form.get('id') or 0)
    except:
        return redirect(url_for('admin_contacts'))
    for c in contacts:
        if c.get('id') == cid:
            c['status'] = 'read'
            break
    save_json(CONTACTS_FILE, contacts)
    return redirect(url_for('admin_contacts'))

@app.route('/admin/stats')
@admin_required
def admin_stats():
    products = load_json(PRODUCTS_FILE) or []
    comments = load_json(COMMENTS_FILE) or []
    contacts = load_json(CONTACTS_FILE) or []
    events = _ensure_analytics_backfill(products, comments)

    from_day, to_day, active_period = _parse_date_range(
        request.args.get('from_date'),
        request.args.get('to_date'),
        request.args.get('period')
    )
    selected_product_id = _to_int(request.args.get('product_id'), 0)
    sort_by = str(request.args.get('sort_by') or 'views').strip().lower()
    sort_dir = str(request.args.get('sort_dir') or 'desc').strip().lower()
    sort_fields = ('name', 'views', 'likes', 'dislikes', 'favorites', 'comments', 'interactions')
    if sort_by not in sort_fields:
        sort_by = 'views'
    if sort_dir not in ('asc', 'desc'):
        sort_dir = 'desc'

    metrics_template = {'views': 0, 'likes': 0, 'dislikes': 0, 'favorites': 0, 'comments': 0}

    def fresh_metrics():
        return metrics_template.copy()

    def in_range(day_key):
        if from_day is None or to_day is None:
            return True
        day_val = _parse_ymd_date(day_key)
        if not day_val:
            return False
        return from_day <= day_val <= to_day

    daily_totals = {}
    product_totals = {}
    product_daily = {}

    for event in events:
        event_type = str(event.get('event_type') or '').strip().lower()
        metric_key = ANALYTICS_EVENT_TYPES.get(event_type)
        day_key = str(event.get('day') or '').strip()
        product_id = _to_int(event.get('product_id'), 0)
        count = _to_int(event.get('count'), 0)
        if not metric_key or not day_key or product_id <= 0 or count <= 0:
            continue
        if not in_range(day_key):
            continue

        by_day = daily_totals.setdefault(day_key, fresh_metrics())
        by_day[metric_key] += count

        by_product = product_totals.setdefault(product_id, fresh_metrics())
        by_product[metric_key] += count

        by_product_day = product_daily.setdefault(product_id, {})
        by_product_day_metrics = by_product_day.setdefault(day_key, fresh_metrics())
        by_product_day_metrics[metric_key] += count

    product_meta = {}
    for product in products:
        product_id = _to_int(product.get('id'), 0)
        if product_id <= 0:
            continue
        product_meta[product_id] = {
            'name': str(product.get('name') or '').strip() or f'ID {product_id}',
            'category': str(product.get('category') or '').strip() or 'Khác',
        }

    all_product_ids = sorted(set(product_meta.keys()) | set(product_totals.keys()))
    product_interaction_rows = []
    for product_id in all_product_ids:
        totals = fresh_metrics()
        raw_totals = product_totals.get(product_id, {})
        for metric_key in metrics_template:
            totals[metric_key] = _to_int(raw_totals.get(metric_key), 0)

        meta = product_meta.get(product_id, {})
        row = {
            'id': product_id,
            'name': meta.get('name') or f'ID {product_id} (đã xóa)',
            'category': meta.get('category') or 'Khác',
            'views': totals['views'],
            'likes': totals['likes'],
            'dislikes': totals['dislikes'],
            'favorites': totals['favorites'],
            'comments': totals['comments'],
        }
        row['interactions'] = row['likes'] + row['dislikes'] + row['favorites'] + row['comments']
        product_interaction_rows.append(row)

    def sort_value(row):
        if sort_by == 'name':
            return str(row.get('name') or '').lower()
        return _to_int(row.get(sort_by), 0)

    product_interaction_rows.sort(key=sort_value, reverse=(sort_dir == 'desc'))

    favorite_product_rows = [r.copy() for r in product_interaction_rows if _to_int(r.get('favorites'), 0) > 0]
    favorite_product_rows.sort(
        key=lambda row: (
            _to_int(row.get('favorites'), 0),
            _to_int(row.get('views'), 0),
            str(row.get('name') or '').lower(),
        ),
        reverse=True
    )

    category_map = {}
    for product_id in all_product_ids:
        meta = product_meta.get(product_id, {})
        category = str(meta.get('category') or '').strip() or 'Khác'
        bucket = category_map.setdefault(category, {
            'category': category,
            'products': 0,
            'views': 0,
            'likes': 0,
            'dislikes': 0,
            'favorites': 0,
            'comments': 0,
            'interactions': 0,
        })
        if product_id in product_meta:
            bucket['products'] += 1
        totals = product_totals.get(product_id, {})
        bucket['views'] += _to_int(totals.get('views'), 0)
        bucket['likes'] += _to_int(totals.get('likes'), 0)
        bucket['dislikes'] += _to_int(totals.get('dislikes'), 0)
        bucket['favorites'] += _to_int(totals.get('favorites'), 0)
        bucket['comments'] += _to_int(totals.get('comments'), 0)
        bucket['interactions'] = bucket['likes'] + bucket['dislikes'] + bucket['favorites'] + bucket['comments']

    category_rows = sorted(
        list(category_map.values()),
        key=lambda row: (
            _to_int(row.get('views'), 0),
            _to_int(row.get('favorites'), 0),
            _to_int(row.get('comments'), 0),
        ),
        reverse=True
    )

    if from_day is not None and to_day is not None:
        all_days = [d.isoformat() for d in _date_iter(from_day, to_day)]
    else:
        all_days = sorted(daily_totals.keys())

    daily_overall_rows = []
    for day_key in reversed(all_days):
        daily = daily_totals.get(day_key, fresh_metrics())
        row = {
            'day': day_key,
            'day_label': _format_day_label(day_key),
            'views': _to_int(daily.get('views'), 0),
            'likes': _to_int(daily.get('likes'), 0),
            'dislikes': _to_int(daily.get('dislikes'), 0),
            'favorites': _to_int(daily.get('favorites'), 0),
            'comments': _to_int(daily.get('comments'), 0),
        }
        row['interactions'] = row['likes'] + row['dislikes'] + row['favorites'] + row['comments']
        daily_overall_rows.append(row)

    selected_product_daily_rows = []
    if selected_product_id > 0:
        selected_daily_map = product_daily.get(selected_product_id, {})
        if from_day is not None and to_day is not None:
            selected_days = [d.isoformat() for d in _date_iter(from_day, to_day)]
        else:
            selected_days = sorted(selected_daily_map.keys())
        for day_key in reversed(selected_days):
            daily = selected_daily_map.get(day_key, fresh_metrics())
            row = {
                'day': day_key,
                'day_label': _format_day_label(day_key),
                'views': _to_int(daily.get('views'), 0),
                'likes': _to_int(daily.get('likes'), 0),
                'dislikes': _to_int(daily.get('dislikes'), 0),
                'favorites': _to_int(daily.get('favorites'), 0),
                'comments': _to_int(daily.get('comments'), 0),
            }
            row['interactions'] = row['likes'] + row['dislikes'] + row['favorites'] + row['comments']
            selected_product_daily_rows.append(row)

    selected_product_comment_rows = []
    if selected_product_id > 0:
        for raw in comments:
            if not isinstance(raw, dict):
                continue
            if _to_int(raw.get('product_id'), 0) != selected_product_id:
                continue
            parsed = _parse_vn_datetime(raw.get('date'))
            if from_day is not None and to_day is not None:
                if not parsed:
                    continue
                if parsed.date() < from_day or parsed.date() > to_day:
                    continue
            selected_product_comment_rows.append({
                'id': _to_int(raw.get('id'), 0),
                'date': str(raw.get('date') or '').strip() or 'Không rõ thời gian',
                'name': str(raw.get('name') or '').strip() or 'Khách',
                'email': str(raw.get('email') or '').strip(),
                'content': str(raw.get('content') or '').strip(),
                'status': str(raw.get('status') or '').strip() or 'pending',
                'rating': _to_int(raw.get('rating'), 0),
                '_parsed': parsed if parsed else datetime.min,
            })
        selected_product_comment_rows.sort(key=lambda row: (row['_parsed'], row['id']), reverse=True)
        for row in selected_product_comment_rows:
            row.pop('_parsed', None)

    overview = {
        'total_views': sum(_to_int(row.get('views'), 0) for row in daily_totals.values()),
        'total_likes': sum(_to_int(row.get('likes'), 0) for row in daily_totals.values()),
        'total_dislikes': sum(_to_int(row.get('dislikes'), 0) for row in daily_totals.values()),
        'total_favorites': sum(_to_int(row.get('favorites'), 0) for row in daily_totals.values()),
        'total_comments': sum(_to_int(row.get('comments'), 0) for row in daily_totals.values()),
        'total_products': len(product_meta),
        'total_contacts': len(contacts),
    }

    product_options = sorted(
        [{'id': pid, 'name': meta.get('name') or f'ID {pid}'} for pid, meta in product_meta.items()],
        key=lambda row: str(row.get('name') or '').lower()
    )
    if selected_product_id not in [row['id'] for row in product_options] and selected_product_id > 0:
        product_options.append({'id': selected_product_id, 'name': f'ID {selected_product_id} (đã xóa)'})

    filter_from = from_day.isoformat() if from_day else ''
    filter_to = to_day.isoformat() if to_day else ''
    base_filters = {
        'from_date': filter_from,
        'to_date': filter_to,
        'period': active_period,
        'product_id': selected_product_id if selected_product_id > 0 else '',
    }

    def build_stats_url(**changes):
        params = base_filters.copy()
        params.update(changes)
        compact = {}
        for key, value in params.items():
            if value is None:
                continue
            text = str(value).strip()
            if text == '':
                continue
            compact[key] = value
        return url_for('admin_stats', **compact)

    sort_links = {}
    for field in sort_fields:
        next_dir = 'asc'
        if sort_by == field and sort_dir == 'asc':
            next_dir = 'desc'
        sort_links[field] = build_stats_url(sort_by=field, sort_dir=next_dir)

    return render_template(
        'admin/stats.html',
        overview=overview,
        daily_overall_rows=daily_overall_rows,
        product_interaction_rows=product_interaction_rows,
        favorite_product_rows=favorite_product_rows,
        category_rows=category_rows,
        selected_product_id=selected_product_id,
        selected_product_daily_rows=selected_product_daily_rows,
        selected_product_comment_rows=selected_product_comment_rows,
        product_options=product_options,
        sort_by=sort_by,
        sort_dir=sort_dir,
        sort_links=sort_links,
        filters={
            'from_date': filter_from,
            'to_date': filter_to,
            'period': active_period,
            'product_id': selected_product_id if selected_product_id > 0 else '',
            'sort_by': sort_by,
            'sort_dir': sort_dir,
        },
        active_page='stats'
    )

# Admin: view and edit shop info
@app.route('/admin/shop-info', methods=['GET', 'POST'])
@admin_required
def admin_shop_info():
    data = load_shop_data()
    shop = data.get('shop', {})
    if request.method == 'POST':
        shop['shop_name'] = (request.form.get('shop_name') or '').strip()
        shop['phone'] = (request.form.get('phone') or '').strip()
        shop['gmail'] = (request.form.get('gmail') or '').strip()
        shop['address'] = (request.form.get('address') or '').strip()
        shop['facebook'] = (request.form.get('facebook') or '').strip()
        shop['description'] = (request.form.get('description') or '').strip()
        # handle logo upload
        img = request.files.get('logo')
        if img and img.filename:
            upload_dir = os.path.join('static', 'uploads')
            os.makedirs(upload_dir, exist_ok=True)
            filename = secure_filename(img.filename)
            save_path = os.path.join(upload_dir, filename)
            img.save(save_path)
            # store accessible path
            shop['logo'] = '/static/uploads/' + filename
        data['shop'] = shop
        save_shop_data(data)
        return redirect(url_for('admin_shop_info'))
    return render_template('admin/shopinfo.html', shop=shop, active_page='shop')


# Admin: categories list/edit/add/delete using data.json
@app.route('/admin/categories')
@admin_required
def admin_categories():
    data = load_shop_data()
    cats = data.get('categories', []) or []
    return render_template('admin/categories.html', categories=cats, active_page='categories')

@app.route('/admin/categories/add', methods=['POST'])
@admin_required
def admin_add_category():
    name = (request.form.get('name') or '').strip()
    slug = (request.form.get('slug') or '').strip()
    if not name:
        return redirect(url_for('admin_categories'))
    data = load_shop_data()
    cats = data.get('categories', [])
    # generate unique 9-digit id
    existing_ids = {int(c.get('id')) for c in cats if str(c.get('id')).isdigit()}
    def gen_id():
        for _ in range(50):
            val = random.randint(100000000, 999999999)
            if val not in existing_ids:
                return val
        # fallback to max+1 if unlucky
        return max(list(existing_ids)+[100000000]) + 1
    new_id = gen_id()
    if not slug:
        slug = name.lower().replace(' ', '-')
    # default visible True
    cats.append({'id': new_id, 'name': name, 'slug': slug, 'visible': True})
    data['categories'] = cats
    save_shop_data(data)
    return redirect(url_for('admin_categories'))

@app.route('/admin/categories/edit', methods=['POST'])
@admin_required
def admin_edit_category():
    try:
        cid_raw = request.form.get('id') or ''
        cid = int(cid_raw)
    except:
        return redirect(url_for('admin_categories'))
    name = (request.form.get('name') or '').strip()
    slug = (request.form.get('slug') or '').strip()
    if not name:
        return redirect(url_for('admin_categories'))
    data = load_shop_data()
    cats = data.get('categories', [])
    for c in cats:
        try:
            if int(c.get('id', 0)) == cid:
                c['name'] = name
                c['slug'] = slug or name.lower().replace(' ', '-')
                # preserve visible if present
                if 'visible' not in c:
                    c['visible'] = True
                break
        except:
            continue
    data['categories'] = cats
    save_shop_data(data)
    return redirect(url_for('admin_categories'))

@app.route('/admin/categories/toggle', methods=['POST'])
@admin_required
def admin_toggle_category():
    # toggle visible flag, return JSON
    try:
        cid = int(request.form.get('id') or 0)
    except:
        return ('', 400)
    data = load_shop_data()
    cats = data.get('categories', [])
    for c in cats:
        if int(c.get('id', 0)) == cid:
            c['visible'] = not bool(c.get('visible', True))
            new_state = c['visible']
            data['categories'] = cats
            save_shop_data(data)
            return {'success': True, 'visible': new_state}
    return {'success': False, 'message': 'Not found'}

# New: delete category via POST (id in form) - fixes BuildError in templates referencing admin_delete_category
@app.route('/admin/categories/delete', methods=['GET', 'POST'], endpoint='admin_delete_category')
@admin_required
def admin_delete_category():
    try:
        cid = int(request.form.get('id') or 0)
    except:
        return redirect(url_for('admin_categories'))

    data = load_shop_data()
    cats = data.get('categories', [])

    cats = [c for c in cats if int(c.get('id', 0)) != cid]

    data['categories'] = cats
    save_shop_data(data)

    return redirect(url_for('admin_categories'))

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=25113)
