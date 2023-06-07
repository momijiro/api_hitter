from setuptools import setup, find_packages

setup(
    name='api_hitter',  # パッケージの名前
    version='0.5.1',  # パッケージのバージョン
    packages=find_packages(),  # パッケージ内のモジュールやサブパッケージを自動的に見つけてインストール
    install_requires=[  # このパッケージが依存する他のパッケージ
        'requests',
        'pandas',
    ],
)
