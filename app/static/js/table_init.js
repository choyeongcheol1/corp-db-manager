// ============================================================
// 테이블 초기화 JavaScript
// - 타겟 DB 단일 선택
// - INSERT / DELETE 액션
// - 법인코드 컬럼 드롭다운 선택
// ============================================================

// 전역 변수
let initTableList = [];
let initTargetCorpCode = null;
let initTargetCorpName = null;
let initSourceCorpCode = null;

// ============================================================
// 소스 DB 관련
// ============================================================

async function loadInitSourceDatabases() {
    const serverId = document.getElementById('init-source-server').value;
    const select = document.getElementById('init-source-db');
    
    if (!serverId) {
        select.innerHTML = '<option value="">먼저 서버를 선택하세요</option>';
        return;
    }
    
    select.innerHTML = '<option value="">로딩 중...</option>';
    
    try {
        const response = await fetch(`/partials/servers/${serverId}/databases`);
        const html = await response.text();
        select.innerHTML = '<option value="">DB를 선택하세요</option>' + html;
    } catch (error) {
        select.innerHTML = '<option value="">오류 발생</option>';
    }
}

async function onInitSourceDbChange() {
    const serverId = document.getElementById('init-source-server').value;
    const dbName = document.getElementById('init-source-db').value;
    
    if (serverId && dbName) {
        // 소스 DB의 법인 정보 조회
        try {
            const response = await fetch(`/api/table-init/corp-info-by-db?db_name=${encodeURIComponent(dbName)}`);
            const data = await response.json();
            
            if (data.found) {
                initSourceCorpCode = data.corp_code;
                document.getElementById('init-source-info').innerHTML = `
                    <div class="bg-blue-50 border border-blue-200 rounded-lg p-3">
                        <div class="flex items-center gap-2 text-blue-700 font-medium text-sm">
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/>
                            </svg>
                            소스 DB 선택됨
                        </div>
                        <div class="text-sm text-blue-600 mt-1">
                            <span class="font-mono">${dbName}</span><br>
                            법인: ${data.corp_name} (${data.corp_code})
                        </div>
                    </div>
                `;
            } else {
                initSourceCorpCode = null;
                document.getElementById('init-source-info').innerHTML = `
                    <div class="bg-yellow-50 border border-yellow-200 rounded-lg p-3">
                        <div class="text-sm text-yellow-700">
                            <span class="font-mono">${dbName}</span> - 법인 정보 없음
                        </div>
                    </div>
                `;
            }
        } catch (error) {
            initSourceCorpCode = null;
        }
        
        // 테이블 목록 로드
        await loadInitTables();
    } else {
        initSourceCorpCode = null;
        document.getElementById('init-source-info').innerHTML = 
            '<p class="text-gray-500">복사할 데이터의 원본 DB를 선택하세요.</p>';
        document.getElementById('init-table-list').innerHTML = 
            '<div class="p-8 text-center text-gray-400">소스 DB를 선택하면 테이블 목록이 표시됩니다</div>';
    }
}

// ============================================================
// 타겟 DB 관련 (단일 선택)
// ============================================================

async function loadInitTargetDatabases() {
    const serverId = document.getElementById('init-target-server').value;
    const select = document.getElementById('init-target-db');
    
    if (!serverId) {
        select.innerHTML = '<option value="">먼저 서버를 선택하세요</option>';
        return;
    }
    
    select.innerHTML = '<option value="">로딩 중...</option>';
    
    try {
        const response = await fetch(`/partials/servers/${serverId}/databases`);
        const html = await response.text();
        select.innerHTML = '<option value="">DB를 선택하세요</option>' + html;
        
        // 타겟 정보 초기화
        initTargetCorpCode = null;
        initTargetCorpName = null;
        document.getElementById('init-target-info').innerHTML = 
            '<p class="text-gray-500">데이터를 복사할 대상 DB를 선택하세요.</p>';
    } catch (error) {
        select.innerHTML = '<option value="">오류 발생</option>';
    }
}

async function onInitTargetDbChange() {
    const dbName = document.getElementById('init-target-db').value;
    const infoContainer = document.getElementById('init-target-info');
    
    if (!dbName) {
        initTargetCorpCode = null;
        initTargetCorpName = null;
        infoContainer.innerHTML = '<p class="text-gray-500">데이터를 복사할 대상 DB를 선택하세요.</p>';
        return;
    }
    
    // 타겟 DB의 법인 정보 조회
    try {
        const response = await fetch(`/api/table-init/corp-info-by-db?db_name=${encodeURIComponent(dbName)}`);
        const data = await response.json();
        
        if (data.found) {
            initTargetCorpCode = data.corp_code;
            initTargetCorpName = data.corp_name;
            infoContainer.innerHTML = `
                <div class="bg-green-50 border border-green-200 rounded-lg p-3">
                    <div class="flex items-center gap-2 text-green-700 font-medium text-sm">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/>
                        </svg>
                        타겟 DB 선택됨
                    </div>
                    <div class="text-sm text-green-600 mt-1">
                        법인: ${data.corp_name} (${data.corp_code})
                    </div>
                </div>
            `;
        } else {
            initTargetCorpCode = null;
            initTargetCorpName = null;
            infoContainer.innerHTML = `
                <div class="bg-yellow-50 border border-yellow-200 rounded-lg p-3">
                    <div class="text-sm text-yellow-700">
                        법인 정보를 찾을 수 없습니다. 법인코드 치환이 동작하지 않습니다.
                    </div>
                </div>
            `;
        }
    } catch (error) {
        initTargetCorpCode = null;
        initTargetCorpName = null;
        infoContainer.innerHTML = `<div class="text-red-500 text-sm">오류: ${error.message}</div>`;
    }
}

// ============================================================
// 테이블 목록
// ============================================================

async function loadInitTables() {
    const serverId = document.getElementById('init-source-server').value;
    const dbName = document.getElementById('init-source-db').value;
    const container = document.getElementById('init-table-list');
    
    if (!serverId || !dbName) {
        container.innerHTML = '<div class="p-8 text-center text-gray-400">소스 DB를 선택하면 테이블 목록이 표시됩니다</div>';
        return;
    }
    
    container.innerHTML = '<div class="p-8 text-center text-gray-500">테이블 목록 조회 중...</div>';
    
    try {
        const response = await fetch(`/api/table-init/tables/${serverId}/${dbName}`);
        initTableList = await response.json();
        
        renderInitTables();
    } catch (error) {
        container.innerHTML = `<div class="p-8 text-center text-red-500">오류: ${error.message}</div>`;
    }
}

function renderInitTables() {
    const container = document.getElementById('init-table-list');
    const searchKeyword = document.getElementById('init-table-search').value.toLowerCase();
    
    // 테이블명 또는 설명으로 검색 가능
    const filteredList = initTableList.filter(t => 
        !searchKeyword || 
        t.table_name.toLowerCase().includes(searchKeyword) ||
        (t.description && t.description.toLowerCase().includes(searchKeyword))
    );
    
    document.getElementById('init-table-count').textContent = `${filteredList.length}개`;
    
    if (filteredList.length === 0) {
        container.innerHTML = '<div class="p-8 text-center text-gray-400">테이블이 없습니다</div>';
        return;
    }
    
    container.innerHTML = `
        <table class="w-full">
            <thead class="bg-gray-50">
                <tr>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">테이블명</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">설명</th>
                    <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">건수</th>
                    <th class="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">법인코드 컬럼</th>
                    <th class="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">상태</th>
                    <th class="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">액션</th>
                </tr>
            </thead>
            <tbody class="divide-y divide-gray-100">
                ${filteredList.map(t => `
                    <tr class="hover:bg-gray-50" id="row-${t.table_name}">
                        <td class="px-4 py-3 font-mono text-sm">${t.table_name}</td>
                        <td class="px-4 py-3 text-sm text-gray-600 max-w-xs truncate" title="${t.description || ''}">
                            ${t.description || '<span class="text-gray-300">-</span>'}
                        </td>
                        <td class="px-4 py-3 text-right text-sm text-gray-600">${(t.row_count || 0).toLocaleString()}</td>
                        <td class="px-4 py-3 text-center">
                            <select id="corp-col-${t.table_name}" 
                                    onfocus="loadTableColumns('${t.table_name}')"
                                    class="px-2 py-1 text-xs border border-gray-300 rounded focus:outline-none focus:border-primary-500 min-w-[120px]">
                                ${t.corp_code_column 
                                    ? `<option value="${t.corp_code_column}" selected>${t.corp_code_column}</option>`
                                    : `<option value="">선택...</option>`
                                }
                            </select>
                        </td>
                        <td class="px-4 py-3 text-center" id="status-${t.table_name}">
                            <span class="text-gray-400 text-xs">대기</span>
                        </td>
                        <td class="px-4 py-3 text-center">
                            <div class="flex items-center justify-center gap-1">
                                <button onclick="executeTableAction('${t.table_name}', 'INSERT')"
                                        class="px-2 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700 transition"
                                        id="btn-insert-${t.table_name}">
                                    INSERT
                                </button>
                                <button onclick="executeTableAction('${t.table_name}', 'DELETE')"
                                        class="px-2 py-1 bg-red-600 text-white text-xs rounded hover:bg-red-700 transition"
                                        id="btn-delete-${t.table_name}">
                                    DELETE
                                </button>
                            </div>
                        </td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
}

function filterInitTables() {
    renderInitTables();
}

// ============================================================
// 테이블 컬럼 목록 로딩 (법인코드 컬럼 선택용)
// ============================================================

async function loadTableColumns(tableName) {
    const select = document.getElementById(`corp-col-${tableName}`);
    
    // 이미 로딩된 경우 스킵 (옵션이 2개 이상이면 이미 로딩됨)
    if (select.options.length > 1) {
        return;
    }
    
    const serverId = document.getElementById('init-source-server').value;
    const dbName = document.getElementById('init-source-db').value;
    
    if (!serverId || !dbName) return;
    
    // 현재 선택된 값 저장
    const currentValue = select.value;
    
    select.innerHTML = '<option value="">로딩중...</option>';
    
    try {
        const response = await fetch(`/api/table-init/columns/${serverId}/${dbName}/${tableName}`);
        const columns = await response.json();
        
        let options = '<option value="">(없음)</option>';
        columns.forEach(col => {
            const selected = col.column_name === currentValue ? 'selected' : '';
            options += `<option value="${col.column_name}" ${selected}>${col.column_name}</option>`;
        });
        
        select.innerHTML = options;
        
        // 기존 값이 있으면 다시 선택
        if (currentValue) {
            select.value = currentValue;
        }
    } catch (error) {
        select.innerHTML = `<option value="">오류</option>`;
    }
}

// ============================================================
// 테이블 액션 실행 (INSERT / DELETE)
// ============================================================

async function executeTableAction(tableName, action) {
    // 유효성 검사
    const targetDb = document.getElementById('init-target-db').value;
    if (!targetDb) {
        alert('타겟 DB를 선택해주세요.');
        return;
    }
    
    const sourceDb = document.getElementById('init-source-db').value;
    if (!sourceDb) {
        alert('소스 DB를 선택해주세요.');
        return;
    }
    
    // 법인코드 컬럼
    const corpCodeColumn = document.getElementById(`corp-col-${tableName}`).value || null;
    
    // 확인
    const actionText = action === 'INSERT' ? '데이터 복사(INSERT)' : '데이터 삭제(DELETE)';
    const confirmMsg = action === 'INSERT'
        ? `[${tableName}] 테이블을 ${targetDb}에 복사하시겠습니까?\n\n소스: ${sourceDb}\n타겟: ${targetDb}${initTargetCorpCode ? `\n법인코드: ${initSourceCorpCode} → ${initTargetCorpCode}` : ''}`
        : `[${tableName}] 테이블의 데이터를 ${targetDb}에서 삭제하시겠습니까?\n\n⚠️ 이 작업은 되돌릴 수 없습니다!`;
    
    if (!confirm(confirmMsg)) {
        return;
    }
    
    const btnInsert = document.getElementById(`btn-insert-${tableName}`);
    const btnDelete = document.getElementById(`btn-delete-${tableName}`);
    const statusCell = document.getElementById(`status-${tableName}`);
    const row = document.getElementById(`row-${tableName}`);
    
    // 버튼 비활성화
    btnInsert.disabled = true;
    btnDelete.disabled = true;
    btnInsert.classList.add('opacity-50');
    btnDelete.classList.add('opacity-50');
    
    if (action === 'INSERT') {
        btnInsert.textContent = '처리중...';
    } else {
        btnDelete.textContent = '처리중...';
    }
    statusCell.innerHTML = '<span class="text-blue-600 text-xs">진행중...</span>';
    
    const requestData = {
        source_server_id: parseInt(document.getElementById('init-source-server').value),
        source_db_name: sourceDb,
        target_server_id: parseInt(document.getElementById('init-target-server').value),
        target_db_name: targetDb,
        table_name: tableName,
        source_corp_code: initSourceCorpCode || '',
        target_corp_code: initTargetCorpCode || '',
        corp_code_column: corpCodeColumn,
        action: action,
        truncate_before_copy: action === 'INSERT' ? document.getElementById('init-opt-truncate').checked : false,
        replace_corp_code: document.getElementById('init-opt-replace-corp').checked,
        keep_identity: document.getElementById('init-opt-keep-identity').checked
    };
    
    try {
        const response = await fetch('/api/table-init/execute', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestData)
        });
        
        const result = await response.json();
        showInitResult(result, action);
        
        // 상태 업데이트
        if (result.success) {
            if (action === 'INSERT') {
                btnInsert.textContent = '완료 ✓';
                btnInsert.classList.remove('bg-blue-600', 'hover:bg-blue-700');
                btnInsert.classList.add('bg-green-600');
            } else {
                btnDelete.textContent = '완료 ✓';
                btnDelete.classList.remove('bg-red-600', 'hover:bg-red-700');
                btnDelete.classList.add('bg-green-600');
            }
            statusCell.innerHTML = `<span class="text-green-600 text-xs">${action} 완료</span>`;
            row.classList.add('bg-green-50');
        } else {
            btnInsert.textContent = 'INSERT';
            btnDelete.textContent = 'DELETE';
            statusCell.innerHTML = `<span class="text-red-600 text-xs">실패</span>`;
            row.classList.add('bg-red-50');
        }
        
        // 버튼 다시 활성화
        btnInsert.disabled = false;
        btnDelete.disabled = false;
        btnInsert.classList.remove('opacity-50');
        btnDelete.classList.remove('opacity-50');
        
    } catch (error) {
        alert(`${actionText} 실행 중 오류: ` + error.message);
        btnInsert.textContent = 'INSERT';
        btnDelete.textContent = 'DELETE';
        btnInsert.disabled = false;
        btnDelete.disabled = false;
        btnInsert.classList.remove('opacity-50');
        btnDelete.classList.remove('opacity-50');
        statusCell.innerHTML = '<span class="text-red-600 text-xs">오류</span>';
    }
}

// ============================================================
// 결과 모달
// ============================================================

function showInitResult(result, action) {
    const modal = document.getElementById('init-result-modal');
    const content = document.getElementById('init-result-content');
    
    const isInsert = action === 'INSERT';
    const actionText = isInsert ? '데이터 복사' : '데이터 삭제';
    
    content.innerHTML = `
        <div class="p-6">
            <div class="flex items-center justify-between mb-4">
                <h3 class="text-lg font-semibold ${result.success ? 'text-green-700' : 'text-red-700'}">
                    ${result.success ? '✅' : '❌'} ${actionText} ${result.success ? '완료' : '실패'}
                </h3>
                <button onclick="closeInitResultModal()" class="text-gray-400 hover:text-gray-600">
                    <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/>
                    </svg>
                </button>
            </div>
            
            <div class="space-y-3 mb-4">
                <div class="flex justify-between text-sm">
                    <span class="text-gray-500">테이블</span>
                    <span class="font-mono font-medium">${result.table_name}</span>
                </div>
                <div class="flex justify-between text-sm">
                    <span class="text-gray-500">소스 DB</span>
                    <span class="font-mono">${result.source_db}</span>
                </div>
                <div class="flex justify-between text-sm">
                    <span class="text-gray-500">타겟 DB</span>
                    <span class="font-mono">${result.target_db}</span>
                </div>
                ${isInsert ? `
                <div class="flex justify-between text-sm">
                    <span class="text-gray-500">복사 건수</span>
                    <span class="font-medium text-blue-600">${(result.rows_copied || 0).toLocaleString()}</span>
                </div>
                <div class="flex justify-between text-sm">
                    <span class="text-gray-500">치환 건수</span>
                    <span class="font-medium text-blue-600">${(result.rows_replaced || 0).toLocaleString()}</span>
                </div>
                ` : `
                <div class="flex justify-between text-sm">
                    <span class="text-gray-500">삭제 건수</span>
                    <span class="font-medium text-red-600">${(result.rows_deleted || 0).toLocaleString()}</span>
                </div>
                `}
                <div class="flex justify-between text-sm">
                    <span class="text-gray-500">소요 시간</span>
                    <span>${(result.elapsed_seconds || 0).toFixed(2)}초</span>
                </div>
            </div>
            
            ${result.error_message ? `
            <div class="bg-red-50 border border-red-200 rounded-lg p-3 mb-4">
                <div class="text-sm text-red-700">${result.error_message}</div>
            </div>
            ` : ''}
            
            <div class="flex justify-end">
                <button onclick="closeInitResultModal()" class="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300">
                    닫기
                </button>
            </div>
        </div>
    `;
    
    modal.classList.remove('hidden');
}

function closeInitResultModal() {
    document.getElementById('init-result-modal').classList.add('hidden');
}