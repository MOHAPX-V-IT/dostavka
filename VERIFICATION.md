# Проверка 0.1.0-rc.1

2026-10-18, Windows x64, portable Go 1.27.2.

* `go test -count=1 ./...`: PASS, 5 tests. Подпись, dedup, payload conflict, durable restart, inflight recovery, dead letter, manual retry, auth, validation, rollback при неудачной записи, ограничение двух workers и реальный HTTP retry.
* `go vet ./...`: PASS.
* Финальная повторная проверка: `go test -count=1 ./...`, `go vet ./...`, `go build -o data/dostavka-rc.exe .`: PASS (exit 0). Preview использует именно dostavka-rc.exe.
* Запущен реальный binary на 127.0.0.1:8787, `/health`: HTTP 200, version 0.1.0-rc.1.

Испытания не утверждают переносимость дисковой atomicity на сетевые файловые системы или работу нескольких инстанций с общим state. Браузерная проверка проводится отдельно координатором.

Добавлена конфигурация GitHub Actions `.github/workflows/checks.yml`: Go test/vet/build на Ubuntu с Go 1.27.2. YAML проверен локальным парсером; workflow на GitHub ещё не запускался.

После визуального обновления admin.html: Go 5 tests / vet / build повторно PASS; inline JavaScript разобран через Node new Function без исполнения DOM. HTTP200 новой панели подтверждён. Визуальная оценка в браузере передана координатору; у UI-агента CUA surfaces недоступны. Происхождение дизайна сохранено в DESIGN_PROVENANCE.md.
