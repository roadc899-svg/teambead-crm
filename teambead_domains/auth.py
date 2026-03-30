TARGET_FUNCTIONS = ['login_page', 'login_submit', 'logout']

MODULE_SOURCE = 'def login_page(request: Request):\n    if get_current_user(request):\n        return RedirectResponse(url="/dashboard", status_code=302)\n    return HTMLResponse(login_page_html())\n\ndef login_submit(username: str = Form(...), password: str = Form(...)):\n    db = SessionLocal()\n    try:\n        user = db.query(User).filter(User.username == username.strip(), User.is_active == 1).first()\n        if not user or not verify_password(password, user.password_hash):\n            return HTMLResponse(login_page_html("Неверный логин или пароль"), status_code=401)\n    finally:\n        db.close()\n\n    token = create_user_session(username.strip())\n    response = RedirectResponse(url="/dashboard", status_code=302)\n    response.set_cookie(SESSION_COOKIE_NAME, token, httponly=True, samesite="lax", max_age=SESSION_DURATION_DAYS * 24 * 60 * 60)\n    return response\n\ndef logout(request: Request):\n    token = request.cookies.get(SESSION_COOKIE_NAME)\n    delete_user_session(token)\n    response = RedirectResponse(url="/login", status_code=302)\n    response.delete_cookie(SESSION_COOKIE_NAME)\n    return response\n\n'


def bind_domain_routes(ctx):
    scope = dict(ctx)
    exec(MODULE_SOURCE, scope)
    return {name: scope[name] for name in TARGET_FUNCTIONS}
