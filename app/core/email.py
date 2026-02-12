"""
이메일 발송 모듈 (Gmail SMTP)
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
import os
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader

load_dotenv()

# Gmail SMTP 설정
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = os.getenv("SMTP_USER", "schoyc@gmail.com")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "ycqc aspf apim vjxr").replace(" ", "")
SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", "DB 관리 시스템")

# 애플리케이션 URL
APP_URL = os.getenv("APP_URL", "http://localhost:8000")

# Jinja2 템플릿 환경 설정
template_env = Environment(loader=FileSystemLoader("app/templates/email"))


def get_template(template_name: str) -> str:
    """템플릿 파일 로드"""
    try:
        template = template_env.get_template(template_name)
        return template
    except Exception as e:
        print(f"[EMAIL] 템플릿 로드 실패: {template_name} - {str(e)}")
        return None


def send_email(
    to_email: str,
    subject: str,
    html_content: str,
    text_content: Optional[str] = None
) -> bool:
    """
    이메일 발송
    
    Args:
        to_email: 수신자 이메일
        subject: 제목
        html_content: HTML 본문
        text_content: 텍스트 본문 (선택)
    
    Returns:
        성공 여부
    """
    try:
        # 메시지 생성
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{SMTP_FROM_NAME} <{SMTP_USER}>"
        msg["To"] = to_email
        
        # 텍스트 버전 (이메일 클라이언트가 HTML을 지원하지 않을 때)
        if text_content:
            part1 = MIMEText(text_content, "plain", "utf-8")
            msg.attach(part1)
        
        # HTML 버전
        part2 = MIMEText(html_content, "html", "utf-8")
        msg.attach(part2)
        
        # SMTP 연결 및 발송
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()  # TLS 암호화
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, to_email, msg.as_string())
        
        print(f"[EMAIL] 발송 성공: {to_email}")
        return True
        
    except Exception as e:
        print(f"[EMAIL] 발송 실패: {to_email} - {str(e)}")
        return False


def send_verification_email(to_email: str, name: str, token: str) -> bool:
    """
    이메일 인증 메일 발송
    
    Args:
        to_email: 수신자 이메일
        name: 사용자 이름
        token: 인증 토큰
    
    Returns:
        성공 여부
    """
    verify_url = f"{APP_URL}/auth/verify-email?token={token}"
    subject = "[DB 관리 시스템] 이메일 인증을 완료해주세요"
    
    # 템플릿 렌더링
    template = get_template("verification.html")
    if template:
        html_content = template.render(
            name=name,
            email=to_email,
            verify_url=verify_url
        )
    else:
        # 템플릿 로드 실패 시 기본 HTML
        html_content = f"""
        <html>
        <body>
            <h1>이메일 인증</h1>
            <p>{name}님, 안녕하세요!</p>
            <p>아래 링크를 클릭하여 이메일 인증을 완료해주세요.</p>
            <p><a href="{verify_url}">이메일 인증하기</a></p>
            <p>이 링크는 24시간 동안만 유효합니다.</p>
        </body>
        </html>
        """
    
    text_content = f"""
{name}님, 안녕하세요!

DB 관리 시스템 회원가입을 환영합니다.
아래 링크를 클릭하여 이메일 인증을 완료해주세요.

인증 링크: {verify_url}

이 링크는 24시간 동안만 유효합니다.
본인이 요청하지 않은 경우 이 메일을 무시하세요.

© 2026 DB 관리 시스템
"""
    
    return send_email(to_email, subject, html_content, text_content)


def send_approval_notification(to_email: str, name: str, approved: bool, reason: Optional[str] = None) -> bool:
    """
    가입 승인/거부 알림 메일 발송
    
    Args:
        to_email: 수신자 이메일
        name: 사용자 이름
        approved: 승인 여부
        reason: 거부 사유 (거부 시)
    
    Returns:
        성공 여부
    """
    login_url = f"{APP_URL}/auth/login"
    
    if approved:
        subject = "[DB 관리 시스템] 가입이 승인되었습니다"
    else:
        subject = "[DB 관리 시스템] 가입이 거부되었습니다"
    
    # 템플릿 렌더링
    template = get_template("approval.html")
    if template:
        html_content = template.render(
            name=name,
            approved=approved,
            reason=reason,
            login_url=login_url
        )
    else:
        # 템플릿 로드 실패 시 기본 HTML
        status = "승인" if approved else "거부"
        html_content = f"""
        <html>
        <body>
            <h1>가입 심사 결과: {status}</h1>
            <p>{name}님, 안녕하세요!</p>
            <p>가입 신청이 {status}되었습니다.</p>
            {"<p><a href='" + login_url + "'>로그인하기</a></p>" if approved else ""}
            {f"<p>사유: {reason}</p>" if reason else ""}
        </body>
        </html>
        """
    
    text_content = f"""
{name}님, 안녕하세요!

가입 신청이 {"승인" if approved else "거부"}되었습니다.
{f"사유: {reason}" if reason and not approved else ""}
{"이제 로그인하여 시스템을 사용할 수 있습니다." if approved else ""}

© 2026 DB 관리 시스템
"""
    
    return send_email(to_email, subject, html_content, text_content)

async def send_password_reset_email(
    to_email: str,
    username: str,
    name: str,
    reset_token: str
):
    """
    비밀번호 재설정 이메일 발송
    """
    import os
    
    APP_URL = os.getenv("APP_URL", "http://localhost:8000")
    reset_link = f"{APP_URL}/auth/reset-password?token={reset_token}"
    
    subject = "[법인 DB 관리 시스템] 비밀번호 재설정"
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"></head>
    <body style="margin: 0; padding: 0; font-family: 'Malgun Gothic', sans-serif; background-color: #f5f5f5;">
        <table width="100%" cellspacing="0" cellpadding="0" style="background-color: #f5f5f5;">
            <tr>
                <td align="center" style="padding: 40px 20px;">
                    <table width="600" cellspacing="0" cellpadding="0" style="background-color: #ffffff; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
                        <tr>
                            <td style="background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%); padding: 32px 40px; border-radius: 8px 8px 0 0;">
                                <h1 style="margin: 0; color: #ffffff; font-size: 24px;">비밀번호 재설정</h1>
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 40px;">
                                <p style="margin: 0 0 20px; color: #374151; font-size: 16px;">
                                    안녕하세요, <strong>{name}</strong>님.
                                </p>
                                <p style="margin: 0 0 20px; color: #374151; font-size: 16px;">
                                    비밀번호 재설정 요청이 접수되었습니다.<br>
                                    아래 버튼을 클릭하여 새 비밀번호를 설정해 주세요.
                                </p>
                                <table width="100%" style="background-color: #f8fafc; border-radius: 8px; margin: 24px 0;">
                                    <tr>
                                        <td style="padding: 20px;">
                                            <p style="margin: 0; color: #6b7280; font-size: 14px;">아이디: <strong style="color: #111827;">{username}</strong></p>
                                        </td>
                                    </tr>
                                </table>
                                <table width="100%">
                                    <tr>
                                        <td align="center" style="padding: 24px 0;">
                                            <a href="{reset_link}" style="display: inline-block; padding: 16px 48px; background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%); color: #ffffff; text-decoration: none; font-size: 16px; font-weight: 600; border-radius: 8px;">
                                                비밀번호 재설정하기
                                            </a>
                                        </td>
                                    </tr>
                                </table>
                                <table width="100%" style="background-color: #fef3c7; border-radius: 8px; margin: 24px 0;">
                                    <tr>
                                        <td style="padding: 16px 20px;">
                                            <p style="margin: 0; color: #92400e; font-size: 14px;">
                                                <strong>주의사항</strong><br>
                                                - 이 링크는 <strong>1시간</strong> 동안만 유효합니다.<br>
                                                - 본인이 요청하지 않았다면 이 이메일을 무시하세요.
                                            </p>
                                        </td>
                                    </tr>
                                </table>
                                <p style="margin: 24px 0 0; color: #9ca3af; font-size: 12px; word-break: break-all;">
                                    버튼이 작동하지 않으면: <a href="{reset_link}" style="color: #3b82f6;">{reset_link}</a>
                                </p>
                            </td>
                        </tr>
                        <tr>
                            <td style="background-color: #f8fafc; padding: 24px 40px; border-radius: 0 0 8px 8px; border-top: 1px solid #e5e7eb;">
                                <p style="margin: 0; color: #9ca3af; font-size: 12px; text-align: center;">
                                    © 2026 법인 DB 관리 시스템
                                </p>
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """
    
    send_email(to_email, subject, html_content)