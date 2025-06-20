#!/bin/bash

# =============================================================================
#  import_messages.sh
#
#  Автоматически импортирует полные JMS-сообщения из XML-файла в ActiveMQ-очередь.
#  Обрабатывает header, properties и body из исходных JMS-сообщений.
#  (Работает с ActiveMQ-CLI v0.9.2, используя опцию --cmdfile.)
#
#  Шаги:
#    1) Проверяем аргументы: XML-файл, queue_name, broker_alias.
#    2) Создаём (или пересоздаём) временный каталог WORK_DIR.
#    3) С помощью xmlstarlet извлекаем все <jms-message> блоки.
#    4) Для каждого сообщения создаём полный JMS-XML с header, properties и body.
#    5) Для каждого сообщения создаём cli_script_N.txt с командами и отправляем.
#    6) Собираем статистику и выводим отчёт.
#
#  Зависимости:
#    • xmlstarlet
#    • java + activemq-cli-0.9.2.jar (и его зависимости) в папке lib/*
#    • В conf/activemq-cli.config должен быть описан брокер с именем <broker_alias>.
#
#  Запуск:
#    chmod +x import_messages.sh
#    ./import_messages.sh input/messages.xml example.queue test
# =============================================================================

set -o pipefail
# set -x  # Для отладки

### 1. Проверяем аргументы
if [ $# -ne 3 ]; then
    echo "Использование: $0 <xml_file> <queue_name> <broker_alias>"
    echo "  где <broker_alias> — имя, указанное в conf/activemq-cli.config"
    echo "Пример: $0 input/messages.xml example.queue test"
    exit 1
fi

XML_FILE="$1"
QUEUE_NAME="$2"
BROKER_ALIAS="$3"

if [ ! -f "$XML_FILE" ]; then
    echo "Ошибка: файл '$XML_FILE' не найден."
    exit 1
fi

### 2. Создаём временный каталог
WORK_DIR="/tmp/amq_import_enhanced_$$"
if [ -d "$WORK_DIR" ]; then
    rm -rf "$WORK_DIR"
fi
mkdir -p "$WORK_DIR"

### 3. Проверяем зависимости
if ! command -v xmlstarlet >/dev/null 2>&1; then
    echo "Ошибка: утилита 'xmlstarlet' не найдена. Установите её: sudo apt-get install xmlstarlet"
    rm -rf "$WORK_DIR"
    exit 1
fi

if ! command -v java >/dev/null 2>&1; then
    echo "Ошибка: 'java' не найдена в PATH."
    rm -rf "$WORK_DIR"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLI_CP="$SCRIPT_DIR/lib/*"
if ! ls $CLI_CP >/dev/null 2>&1; then
    echo "Ошибка: не найдены JAR-файлы ActiveMQCLI в '$SCRIPT_DIR/lib/'."
    rm -rf "$WORK_DIR"
    exit 1
fi

echo ""
echo "[1/3] Анализируем структуру XML-файла '$XML_FILE'…"

# Подсчитываем количество JMS-сообщений
msg_count=$(xmlstarlet sel -t -v "count(//jms-message)" "$XML_FILE" 2>/dev/null)

echo "  → Найдено JMS-сообщений: $msg_count"
if [ "$msg_count" -eq 0 ]; then
    echo "  Ошибка: теги <jms-message> не найдены в файле."
    rm -rf "$WORK_DIR"
    exit 1
fi

echo "[2/3] Извлекаем и обрабатываем JMS-сообщения…"

### 4. Функция для извлечения одного JMS-сообщения
extract_jms_message() {
    local msg_num="$1"
    local output_file="$2"

    echo "  → [${msg_num}/${msg_count}] Извлекаем сообщение $msg_num"

    # Создаем временный файл для сырого извлечения
    local raw_file="$WORK_DIR/raw_message_${msg_num}.xml"

    # Извлекаем только одно JMS-сообщение целиком с сохранением структуры
    xmlstarlet sel -t -m "//jms-message[$msg_num]" -c . "$XML_FILE" > "$raw_file" 2>/dev/null

    if [ ! -s "$raw_file" ]; then
        echo "    ✗ Ошибка извлечения сообщения $msg_num"
        return 1
    fi

    # Оборачиваем в корневой элемент jms-messages и добавляем XML заголовок
    # Используем EOM_INNER для внутреннего here-document, чтобы не конфликтовать с EOF_NEW_FUNCTION_CODE
    cat > "$output_file" <<EOM_INNER
<?xml version="1.0" encoding="UTF-8"?>
<jms-messages>
EOM_INNER

    # Добавляем извлеченное сообщение с правильными отступами
    sed 's/^/  /' "$raw_file" >> "$output_file"

    cat >> "$output_file" <<EOM_INNER
</jms-messages>
EOM_INNER

    # Удаляем временный файл
    rm -f "$raw_file"

    if [ ! -s "$output_file" ]; then
        echo "    ✗ Ошибка создания обернутого сообщения $msg_num"
        return 1
    fi

    # --- НАЧАЛО ИСПРАВЛЕНИЯ ---
    # Заменяем пустые <value/> на <value>0</value> для известных числовых свойств AMQ
    # чтобы избежать ошибки "For input string: """ в ActiveMQ CLI
    local temp_fixed_file="${output_file}.fixed"
    # Важно: $WORK_DIR здесь должен быть \$WORK_DIR, чтобы он использовался из скрипта, а не из текущей сессии терминала
    xmlstarlet ed \
        -u "//jms-message/properties/property[name='AMQ_SCHEDULED_DELAY']/value[not(node()) and not(text())]" -v "0" \
        -u "//jms-message/properties/property[name='AMQ_SCHEDULED_REPEAT']/value[not(node()) and not(text())]" -v "0" \
        -u "//jms-message/properties/property[name='AMQ_SCHEDULED_PERIOD']/value[not(node()) and not(text())]" -v "0" \
        "$output_file" > "$temp_fixed_file" 2>"$WORK_DIR/xmlstarlet_ed_err_${msg_num}.log" # Добавил msg_num в имя лога ошибок

    if [ -s "$temp_fixed_file" ]; then
        mv "$temp_fixed_file" "$output_file"
    else
        if [ -f "$temp_fixed_file" ]; then # Если файл создан, но пуст
            rm -f "$temp_fixed_file"
        fi
        # echo "    ~ Замечание: xmlstarlet не внес изменений для пустых числовых свойств AMQ_SCHEDULED_* или произошла ошибка при попытке."
        # Можно проверить $WORK_DIR/xmlstarlet_ed_err_${msg_num}.log
    fi
    # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

    # Проверяем валидность XML
    if ! xmlstarlet val "$output_file" >/dev/null 2>&1; then
        echo "    ✗ Созданный XML невалидный для сообщения $msg_num (возможно, после исправления пустых свойств)"
        echo "    > Первые строки невалидного XML:"
        head -n 10 "$output_file" | sed 's/^/      /'
        return 1
    fi

    echo "    ✓ Размер: $(wc -c < "$output_file") байт (валидный XML)"
    return 0
}

# Функция для проверки формата XML сообщения
validate_message_format() {
    local xml_file="$1"
    local msg_num="$2"

    echo "    > Проверяем формат сообщения:"

    # Проверяем наличие обязательных элементов
    if ! xmlstarlet sel -t -v "count(//jms-message)" "$xml_file" | grep -q "1"; then
        echo "      ✗ Неверное количество jms-message элементов"
        return 1
    fi

    if ! xmlstarlet sel -t -v "count(//jms-message/header)" "$xml_file" | grep -q "1"; then
        echo "      ✗ Отсутствует header"
        return 1
    fi

    if ! xmlstarlet sel -t -v "count(//jms-message/body)" "$xml_file" | grep -q "1"; then
        echo "      ✗ Отсутствует body"
        return 1
    fi

    # Проверяем содержимое body
    local body_content=$(xmlstarlet sel -t -v "//jms-message/body" "$xml_file" 2>/dev/null)
    if [ -z "$body_content" ]; then
        echo "      ⚠ Body пустой"
    else
        local body_size=${#body_content}
        echo "      ✓ Body содержит $body_size символов"
    fi

    # Проверяем destination
    local destination=$(xmlstarlet sel -t -v "//jms-message/header/destination" "$xml_file" 2>/dev/null)
    echo "      → Destination: $destination"

    # Проверяем message-id
    local message_id=$(xmlstarlet sel -t -v "//jms-message/header/message-id" "$xml_file" 2>/dev/null)
    echo "      → Message-ID: $message_id"

    return 0
}

### 5. Функция отправки сообщения
send_jms_message() {
    local wrapper_file="$1"
    local msg_num="$2"

    local log_file="$WORK_DIR/output_${msg_num}.log"
    local cli_script="$WORK_DIR/cli_script_${msg_num}.txt"

    echo "  → [${msg_num}/${msg_count}] Отправляем JMS-сообщение $msg_num"

    # Создаём CLI-скрипт ТОЛЬКО для отправки
    # Используем EOM_INNER для here-document, чтобы не конфликтовать с EOF_NEW_SEND_FUNCTION_CODE
    cat > "$cli_script" <<EOM_INNER_SCRIPT
connect --broker $BROKER_ALIAS
send-message --queue $QUEUE_NAME --file $wrapper_file
disconnect
exit
EOM_INNER_SCRIPT

    # Запускаем CLI с timeout
    java -cp "$CLI_CP" activemq.cli.ActiveMQCLI --cmdfile "$cli_script" > "$log_file" 2>&1 &
    java_pid=$!

    # Ждем завершения максимум 30 секунд
    timeout_count=0
    while kill -0 $java_pid 2>/dev/null && [ $timeout_count -lt 30 ]; do
        sleep 1
        ((timeout_count++))
    done

    if kill -0 $java_pid 2>/dev/null; then
        echo "Timeout: принудительно завершаем процесс $java_pid" >> "$log_file"
        kill -9 $java_pid 2>/dev/null
        echo "    ✗ Timeout (30 сек)"
        cat "$log_file" | sed 's/^/      /' # Показать лог при таймауте
        return 1
    fi

    wait $java_pid
    java_exit_code=$?

    # Детальный анализ лога
    echo "    > Полный лог отправки:"
    cat "$log_file" | sed 's/^/      /'

    # Ключевой момент: проверяем, было ли сообщение отправлено, НЕЗАВИСИМО от кода возврата CLI,
    # так как последующие команды (disconnect/exit) или сама `queue-stats` могли вызывать ошибку.
    if grep -q "Messages sent to queue" "$log_file"; then
        echo "    ✓ Успешно отправлено (подтверждено по логу 'Messages sent to queue')"
        # Если код возврата был не 0, но сообщение отправлено, все равно сообщим об этом, но не будем считать ошибкой отправки.
        if [ $java_exit_code -ne 0 ]; then
            echo "    ⚠ Java CLI вернул код $java_exit_code, но сообщение было отправлено. Возможна проблема с disconnect/exit или внутренняя ошибка CLI после отправки."
        fi
        return 0 # Успех отправки
    else
        # Если сообщение не было отправлено, тогда это точно ошибка
        if [ $java_exit_code -eq 0 ]; then
            # Это странный случай: CLI вернул 0, но нет подтверждения отправки
            echo "    ✗ Java CLI вернул код 0, но нет подтверждения отправки в логе ('Messages sent to queue'). Проверьте лог."
        else
            echo "    ✗ Java CLI вернул код $java_exit_code и нет подтверждения отправки в логе."
        fi
        # Дополнительно ищем другие явные ошибки, если нет "Messages sent to queue"
        if grep -qiE "error|exception|failed|unable|invalid|cannot|could not|reference.*not.*allowed|parse.*error|malformed" "$log_file"; then
             echo "    ✗ Найдены явные ошибки в логе CLI."
        fi
        return 1 # Ошибка отправки
    fi
}

### 6. Функция для принудительной очистки процессов
cleanup_java_processes() {
    pkill -f "activemq.cli.ActiveMQCLI" 2>/dev/null || true
    sleep 1
}

### 7. Основной цикл обработки
success_count=0
error_count=0
consecutive_errors=0

# Функция для отладки первых сообщений
debug_first_messages() {
    echo ""
    echo "=== ОТЛАДКА: Анализируем структуру первого сообщения ==="

    # Показываем структуру исходного XML
    echo "Структура исходного файла:"
    xmlstarlet el "$XML_FILE" | head -10 | sed 's/^/  /'

    # Показываем первое сообщение
    echo ""
    echo "Первое JMS-сообщение из исходного файла:"
    xmlstarlet sel -t -m "//jms-message[1]" -c . "$XML_FILE" | xmlstarlet fo | head -20 | sed 's/^/  /'

    # Создаем тестовое сообщение
    local test_file="$WORK_DIR/test_message.xml"
    if extract_jms_message 1 "$test_file"; then
        echo ""
        echo "Сгенерированное сообщение для ActiveMQ:"
        head -20 "$test_file" | sed 's/^/  /'

        # Проверяем валидность
        if xmlstarlet val "$test_file" >/dev/null 2>&1; then
            echo "  ✓ XML валидный"
        else
            echo "  ✗ XML невалидный"
            xmlstarlet val "$test_file" 2>&1 | head -3 | sed 's/^/    /'
        fi
    fi
    echo "=================================================="
}

# Запускаем отладку перед основным циклом
debug_first_messages

for (( msg_num=1; msg_num<=msg_count; msg_num++ )); do
    wrapper_file="$WORK_DIR/jms_message_${msg_num}.xml"

    echo ""
    echo "=== Обработка сообщения $msg_num из $msg_count ==="

    # Извлекаем JMS-сообщение
    if ! extract_jms_message "$msg_num" "$wrapper_file"; then
        echo "  ✗ Ошибка извлечения сообщения $msg_num"
        ((error_count++))
        ((consecutive_errors++))
        continue
    fi

    # Проверяем формат сообщения
    validate_message_format "$wrapper_file" "$msg_num"

    # Отправляем сообщение
    if send_jms_message "$wrapper_file" "$msg_num"; then
        ((success_count++))
        consecutive_errors=0
    else
        ((error_count++))
        ((consecutive_errors++))

        # Лог уже выводится в функции send_jms_message, поэтому здесь не дублируем

        cleanup_java_processes
    fi

    # Прерываем при множественных ошибках подряд
    if [ "$consecutive_errors" -ge 5 ]; then
        echo ""
        echo "⚠ Критично: $consecutive_errors ошибок подряд. Прерываем обработку."
        break
    fi

    # Небольшая пауза между сообщениями
    sleep 0.3

    # Показываем прогресс
    if [ $((msg_num % 10)) -eq 0 ]; then
        echo ""
        echo ">>> Прогресс: $msg_num/$msg_count (успешно: $success_count, ошибок: $error_count)"
    fi
done

# Финальная очистка процессов
cleanup_java_processes

### 8. Проверка состояния очереди с детальной диагностикой
echo ""
echo "[3/3] Проверяем состояние очереди '$QUEUE_NAME'…"

check_script="$WORK_DIR/check_queue.txt"
check_log="$WORK_DIR/queue_check.log"

cat > "$check_script" <<EOF
connect --broker $BROKER_ALIAS
queue-stats --queue $QUEUE_NAME
list-queues
disconnect
exit
EOF

if java -cp "$CLI_CP" activemq.cli.ActiveMQCLI --cmdfile "$check_script" > "$check_log" 2>&1; then
    echo "  → Результат проверки очереди:"
    cat "$check_log" | sed 's/^/    /'

    # Извлекаем количество сообщений
    if grep -q "Messages" "$check_log"; then
        local queue_messages=$(grep -E "Messages.*[0-9]" "$check_log" | head -1)
        echo "  → Найдено в очереди: $queue_messages"
    fi
else
    echo "  ⚠ Не удалось проверить состояние очереди"
    echo "  → Лог проверки:"
    cat "$check_log" | sed 's/^/    /'
fi

### 9. Итоговый отчёт
echo ""
echo "=================================================="
echo "=== ИТОГОВЫЙ ОТЧЁТ ИМПОРТА JMS-СООБЩЕНИЙ ==="
echo "=================================================="
echo "Исходный файл:        $XML_FILE"
echo "Целевая очередь:      $QUEUE_NAME"
echo "Брокер (алиас):       $BROKER_ALIAS"
echo ""
echo "Всего сообщений:      $msg_count"
echo "Успешно отправлено:   $success_count"
echo "Ошибок:               $error_count"

if [ "$msg_count" -gt 0 ]; then
    success_rate=$(( success_count * 100 / msg_count ))
    echo "Процент успеха:       ${success_rate}%"
fi

if [ "$error_count" -gt 0 ]; then
    echo ""
    echo "=== ОТЛАДОЧНАЯ ИНФОРМАЦИЯ ==="
    echo "Рабочий каталог: $WORK_DIR"
    echo "  • JMS XML-файлы:     $WORK_DIR/jms_message_*.xml"
    echo "  • CLI-скрипты:       $WORK_DIR/cli_script_*.txt"
    echo "  • Логи отправки:     $WORK_DIR/output_*.log"
    echo ""
    echo "Для ручной отладки сообщения N:"
    echo "  java -cp \"$CLI_CP\" activemq.cli.ActiveMQCLI \\"
    echo "    --cmdfile \"$WORK_DIR/cli_script_N.txt\""
    echo ""
    echo "Каталог $WORK_DIR сохранён для анализа ошибок."
else
    echo ""
    echo "🎉 Все JMS-сообщения успешно импортированы!"
    echo "   Включая headers, properties и body из исходного файла."
    rm -rf "$WORK_DIR"
fi

echo ""
echo "Импорт завершен."
exit $error_count