from flask import Flask, render_template_string

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>TEAMBEAD CRM</title>
    <style>
        body { font-family: Arial; background: #0f172a; color: white; }
        h1 { color: #38bdf8; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; }
        th, td { padding: 10px; border: 1px solid #334155; }
        th { background: #1e293b; }
    </style>
</head>
<body>
    <h1>🚀 TEAMBEAD CRM</h1>
    <p>CRM работает!</p>

    <table>
        <tr>
            <th>Оффер</th>
            <th>Гео</th>
            <th>Менеджер</th>
            <th>Депозит</th>
        </tr>
        <tr>
            <td>Mines</td>
            <td>PE</td>
            <td>Ты</td>
            <td>120$</td>
        </tr>
    </table>
</body>
</html>
"""

@app.route("/")
def home():
    return render_template_string(HTML)

if __name__ == "__main__":
    app.run()
