from app.core.database import PgSessionLocal
from app.models.user import User
from app.core.security import verify_password

db = PgSessionLocal()
user = db.query(User).filter(User.username == 'admin2').first()

print('=== admin2 정보 ===')
print(f'username: {user.username}')
print(f'status: {user.status}')
print(f'email_verified: {user.email_verified}')
print(f'is_active: {user.is_active}')
print(f'can_login: {user.can_login}')
print(f'password_hash: {user.password_hash[:30]}...')

# 비밀번호 테스트
test_pw = '0000'
print(f'비밀번호 검증: {verify_password(test_pw, user.password_hash)}')

db.close()