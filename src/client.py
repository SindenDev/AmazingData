import os
import AmazingData as ad
from dotenv import load_dotenv

load_dotenv()


def get_client():
    """登录并返回 AmazingData 模块（登录后所有接口均可用）"""
    ad.login(
        username=os.environ["AD_USERNAME"],
        password=os.environ["AD_PASSWORD"],
        host=os.environ["AD_HOST"],
        port=int(os.environ["AD_PORT"]),
    )
    return ad
