/**
 * LocalStorage 서비스
 * 
 * DB 관리 시스템의 로컬 데이터 저장/관리
 * - 서버 연결 정보
 * - 시스템 설정
 * - 활동 로그
 * 
 * 사용법:
 * const storage = new LocalStorageService();
 * storage.saveServer(serverData);
 * const servers = storage.getServers();
 */

class LocalStorageService {
    constructor() {
        this.KEYS = {
            SERVERS: 'dbms_servers',
            SETTINGS: 'dbms_settings',
            ACTIVITY_LOGS: 'dbms_activity_logs',
            CURRENT_SERVER: 'dbms_current_server',
            USER_PREFERENCES: 'dbms_user_preferences'
        };
        
        // 암호화 키 (실제 운영에서는 더 안전한 방식 사용)
        this.ENCRYPTION_KEY = 'dbms_secure_key_2024';
        
        // 활동 로그 최대 보관 수
        this.MAX_LOGS = 1000;
    }
    
    // ============================================================
    // 암호화/복호화
    // ============================================================
    
    /**
     * 간단한 암호화 (Base64 + 문자 시프트)
     * 실제 운영에서는 Web Crypto API 사용 권장
     */
    encrypt(text) {
        if (!text) return '';
        try {
            const shifted = text.split('').map((char, i) => 
                String.fromCharCode(char.charCodeAt(0) + (i % 5) + 1)
            ).join('');
            return btoa(encodeURIComponent(shifted));
        } catch (e) {
            console.error('암호화 실패:', e);
            return '';
        }
    }
    
    decrypt(encoded) {
        if (!encoded) return '';
        try {
            const shifted = decodeURIComponent(atob(encoded));
            return shifted.split('').map((char, i) => 
                String.fromCharCode(char.charCodeAt(0) - (i % 5) - 1)
            ).join('');
        } catch (e) {
            console.error('복호화 실패:', e);
            return '';
        }
    }
    
    // ============================================================
    // 기본 저장/조회
    // ============================================================
    
    _save(key, data) {
        try {
            localStorage.setItem(key, JSON.stringify(data));
            return true;
        } catch (e) {
            console.error('저장 실패:', e);
            return false;
        }
    }
    
    _load(key, defaultValue = null) {
        try {
            const data = localStorage.getItem(key);
            return data ? JSON.parse(data) : defaultValue;
        } catch (e) {
            console.error('로드 실패:', e);
            return defaultValue;
        }
    }
    
    _remove(key) {
        try {
            localStorage.removeItem(key);
            return true;
        } catch (e) {
            console.error('삭제 실패:', e);
            return false;
        }
    }
    
    // ============================================================
    // 서버 관리
    // ============================================================
    
    /**
     * 모든 서버 목록 조회
     */
    getServers() {
        const servers = this._load(this.KEYS.SERVERS, []);
        // 비밀번호 복호화
        return servers.map(server => ({
            ...server,
            password: server.save_password ? this.decrypt(server.password) : ''
        }));
    }
    
    /**
     * 서버 저장 (추가/수정)
     */
    saveServer(serverData) {
        const servers = this._load(this.KEYS.SERVERS, []);
        
        // 비밀번호 암호화
        const dataToSave = {
            ...serverData,
            password: serverData.save_password ? this.encrypt(serverData.password) : '',
            updated_at: new Date().toISOString()
        };
        
        if (serverData.id) {
            // 수정
            const index = servers.findIndex(s => s.id === serverData.id);
            if (index !== -1) {
                // 비밀번호가 비어있으면 기존 유지
                if (!serverData.password && servers[index].password) {
                    dataToSave.password = servers[index].password;
                }
                servers[index] = { ...servers[index], ...dataToSave };
            }
        } else {
            // 추가
            dataToSave.id = this._generateId();
            dataToSave.created_at = new Date().toISOString();
            dataToSave.is_active = true;
            servers.push(dataToSave);
        }
        
        this._save(this.KEYS.SERVERS, servers);
        return dataToSave;
    }
    
    /**
     * 서버 조회 (단일)
     */
    getServer(serverId) {
        const servers = this.getServers();
        return servers.find(s => s.id === serverId) || null;
    }
    
    /**
     * 서버 삭제
     */
    deleteServer(serverId) {
        const servers = this._load(this.KEYS.SERVERS, []);
        const filtered = servers.filter(s => s.id !== serverId);
        this._save(this.KEYS.SERVERS, filtered);
        
        // 현재 선택된 서버였다면 해제
        const current = this.getCurrentServer();
        if (current && current.id === serverId) {
            this._remove(this.KEYS.CURRENT_SERVER);
        }
        
        return true;
    }
    
    /**
     * 현재 선택된 서버 설정
     */
    setCurrentServer(serverId) {
        const server = this.getServer(serverId);
        if (server) {
            this._save(this.KEYS.CURRENT_SERVER, { id: serverId, selected_at: new Date().toISOString() });
            return true;
        }
        return false;
    }
    
    /**
     * 현재 선택된 서버 조회
     */
    getCurrentServer() {
        const current = this._load(this.KEYS.CURRENT_SERVER);
        if (current && current.id) {
            return this.getServer(current.id);
        }
        return null;
    }
    
    // ============================================================
    // 설정 관리
    // ============================================================
    
    /**
     * 전체 설정 조회
     */
    getSettings() {
        return this._load(this.KEYS.SETTINGS, this._getDefaultSettings());
    }
    
    /**
     * 전체 설정 저장
     */
    saveSettings(settings) {
        const current = this.getSettings();
        const merged = { ...current, ...settings, updated_at: new Date().toISOString() };
        
        // ERP DB 비밀번호 암호화
        if (merged.erp && merged.erp.password) {
            merged.erp.password = this.encrypt(merged.erp.password);
        }
        
        this._save(this.KEYS.SETTINGS, merged);
        return merged;
    }
    
    /**
     * 특정 설정 섹션 조회
     */
    getSetting(section) {
        const settings = this.getSettings();
        const value = settings[section];
        
        // ERP 비밀번호 복호화
        if (section === 'erp' && value && value.password) {
            return { ...value, password: this.decrypt(value.password) };
        }
        
        return value;
    }
    
    /**
     * 특정 설정 섹션 저장
     */
    saveSetting(section, value) {
        const settings = this.getSettings();
        settings[section] = value;
        return this.saveSettings(settings);
    }
    
    /**
     * 기본 설정값
     */
    _getDefaultSettings() {
        return {
            // 알림 설정
            alerts: {
                capacity_warning: 80,
                capacity_critical: 95,
                enable_notifications: true
            },
            // 복제 설정
            replication: {
                template_db: '',
                data_path: '',
                log_path: '',
                db_account_id: '',
                db_password: ''
            },
            // ERP 연동 설정
            erp: {
                enabled: false,
                host: '',
                port: 1433,
                database: '',
                username: '',
                password: ''
            },
            // UI 설정
            ui: {
                theme: 'light',
                sidebar_collapsed: false,
                items_per_page: 20
            },
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        };
    }
    
    // ============================================================
    // 활동 로그
    // ============================================================
    
    /**
     * 활동 로그 추가
     */
    addLog(action, target, detail = '') {
        const logs = this._load(this.KEYS.ACTIVITY_LOGS, []);
        
        const log = {
            id: this._generateId(),
            action: action,        // CREATE, UPDATE, DELETE, SELECT, TEST, etc.
            target: target,        // 대상 (서버명, DB명 등)
            detail: detail,        // 상세 내용
            timestamp: new Date().toISOString()
        };
        
        logs.unshift(log);  // 최신순
        
        // 최대 개수 제한
        if (logs.length > this.MAX_LOGS) {
            logs.splice(this.MAX_LOGS);
        }
        
        this._save(this.KEYS.ACTIVITY_LOGS, logs);
        return log;
    }
    
    /**
     * 활동 로그 조회
     */
    getLogs(options = {}) {
        const { limit = 50, offset = 0, action = null, search = '' } = options;
        let logs = this._load(this.KEYS.ACTIVITY_LOGS, []);
        
        // 필터링
        if (action) {
            logs = logs.filter(log => log.action === action);
        }
        if (search) {
            const keyword = search.toLowerCase();
            logs = logs.filter(log => 
                log.target.toLowerCase().includes(keyword) ||
                log.detail.toLowerCase().includes(keyword)
            );
        }
        
        // 페이징
        const total = logs.length;
        const items = logs.slice(offset, offset + limit);
        
        return { items, total, limit, offset };
    }
    
    /**
     * 활동 로그 전체 삭제
     */
    clearLogs() {
        this._save(this.KEYS.ACTIVITY_LOGS, []);
        return true;
    }
    
    // ============================================================
    // 데이터 내보내기/가져오기
    // ============================================================
    
    /**
     * 전체 데이터 내보내기
     */
    exportData(includePassword = false) {
        const data = {
            version: '1.0',
            exported_at: new Date().toISOString(),
            servers: this._load(this.KEYS.SERVERS, []),
            settings: this._load(this.KEYS.SETTINGS, {}),
            logs: this._load(this.KEYS.ACTIVITY_LOGS, [])
        };
        
        // 비밀번호 제외 옵션
        if (!includePassword) {
            data.servers = data.servers.map(s => ({ ...s, password: '' }));
            if (data.settings.erp) {
                data.settings.erp = { ...data.settings.erp, password: '' };
            }
            if (data.settings.replication) {
                data.settings.replication = { ...data.settings.replication, db_password: '' };
            }
        }
        
        return data;
    }
    
    /**
     * 데이터 내보내기 (파일 다운로드)
     */
    downloadExport(filename = 'dbms_backup.json', includePassword = false) {
        const data = this.exportData(includePassword);
        const json = JSON.stringify(data, null, 2);
        const blob = new Blob([json], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }
    
    /**
     * 데이터 가져오기
     */
    importData(jsonData, options = {}) {
        const { 
            overwrite = false,      // 기존 데이터 덮어쓰기
            importServers = true, 
            importSettings = true, 
            importLogs = false 
        } = options;
        
        try {
            const data = typeof jsonData === 'string' ? JSON.parse(jsonData) : jsonData;
            
            if (importServers && data.servers) {
                if (overwrite) {
                    this._save(this.KEYS.SERVERS, data.servers);
                } else {
                    // 병합 (ID 중복 체크)
                    const existing = this._load(this.KEYS.SERVERS, []);
                    const existingIds = new Set(existing.map(s => s.id));
                    const newServers = data.servers.filter(s => !existingIds.has(s.id));
                    this._save(this.KEYS.SERVERS, [...existing, ...newServers]);
                }
            }
            
            if (importSettings && data.settings) {
                if (overwrite) {
                    this._save(this.KEYS.SETTINGS, data.settings);
                } else {
                    const existing = this.getSettings();
                    this._save(this.KEYS.SETTINGS, { ...existing, ...data.settings });
                }
            }
            
            if (importLogs && data.logs) {
                if (overwrite) {
                    this._save(this.KEYS.ACTIVITY_LOGS, data.logs);
                } else {
                    const existing = this._load(this.KEYS.ACTIVITY_LOGS, []);
                    const merged = [...data.logs, ...existing].slice(0, this.MAX_LOGS);
                    this._save(this.KEYS.ACTIVITY_LOGS, merged);
                }
            }
            
            return { success: true, message: '데이터를 가져왔습니다.' };
        } catch (e) {
            console.error('가져오기 실패:', e);
            return { success: false, message: '데이터 형식이 올바르지 않습니다.' };
        }
    }
    
    /**
     * 파일에서 데이터 가져오기
     */
    importFromFile(file, options = {}) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e) => {
                const result = this.importData(e.target.result, options);
                resolve(result);
            };
            reader.onerror = () => reject({ success: false, message: '파일 읽기 실패' });
            reader.readAsText(file);
        });
    }
    
    // ============================================================
    // 초기화
    // ============================================================
    
    /**
     * 전체 데이터 초기화
     */
    clearAll() {
        Object.values(this.KEYS).forEach(key => this._remove(key));
        return true;
    }
    
    /**
     * 특정 데이터 초기화
     */
    clear(type) {
        const key = this.KEYS[type.toUpperCase()];
        if (key) {
            this._remove(key);
            return true;
        }
        return false;
    }
    
    // ============================================================
    // 유틸리티
    // ============================================================
    
    /**
     * 고유 ID 생성
     */
    _generateId() {
        return Date.now().toString(36) + Math.random().toString(36).substr(2, 9);
    }
    
    /**
     * 저장소 사용량 확인
     */
    getStorageUsage() {
        let total = 0;
        Object.values(this.KEYS).forEach(key => {
            const item = localStorage.getItem(key);
            if (item) {
                total += item.length * 2; // UTF-16
            }
        });
        return {
            used: total,
            usedMB: (total / 1024 / 1024).toFixed(2),
            limit: 5 * 1024 * 1024, // 대부분의 브라우저 5MB
            limitMB: 5
        };
    }
}

// 전역 인스턴스
const storageService = new LocalStorageService();

// ES Module export (필요 시)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { LocalStorageService, storageService };
}