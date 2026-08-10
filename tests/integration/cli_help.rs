#[test]
fn help_lists_diagnostics_commands_and_status_json_option() {
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("submit"));
    assert!(text.contains("daemon"));
    assert!(text.contains("events"));
    assert!(text.contains("inspect"));
    assert!(text.contains("explain"));
    assert!(text.contains("doctor"));

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["status", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("--json"));
}

#[test]
fn events_rejects_limits_outside_the_diagnostic_bound() {
    let zero = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["events", "--limit", "0"])
        .output()
        .unwrap();

    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr)
        .contains("diagnostic event limit must be between 1 and 1000"));

    let too_large = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["events", "--limit", "1001"])
        .output()
        .unwrap();

    assert!(!too_large.status.success());
    assert!(String::from_utf8_lossy(&too_large.stderr)
        .contains("diagnostic event limit must be between 1 and 1000"));
}

#[test]
fn steer_help_describes_enqueue_and_bounded_list_options() {
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("steer"));

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["steer", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("list"));
    assert!(text.contains("MESSAGE"));
    assert!(text.contains("--json"));

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["steer", "list", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("--json"));

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .arg("steer")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("required arguments were not provided")
    );
}

#[test]
fn readme_documents_human_intervention_workflow() {
    let readme = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))
        .expect("README.md should be readable");

    for clause in [
        r#"pueue-agent steer -- "次は learning rate を半分にして""#,
        "SQLite へ登録するだけで、agent の起動、Pueue 操作、実行中 process への入力は行いません",
        "FIFO 順で一度だけ、次回の agent run の prompt に渡されますが、1回ですべての pending メッセージを配信するとは限りません",
        "各メッセージは最大 `4,096 bytes` です。1回の run には最大 `16 messages`、合計 `16,384 intervention bytes` までを、残りの prompt budget に収まる範囲で配信します",
        "上限または残りの prompt budget を超える FIFO の後続メッセージ（超過分）は pending のまま、後続の run へ繰り越されます",
        "agent の spawn に失敗した場合、メッセージは pending に戻されるため、次回の run で再試行されます",
        "`pause` または `disable` 中でもメッセージはキューへ登録できますが、配信はせず、resume または enable 後の次回 run まで保持されます",
        "`status --json` はキューの件数などの診断情報を返しますが、メッセージ本文は含めません",
        "実行中の agent は中断しません",
        "介入メッセージによって、安全ポリシーや既存の制約を上書きすることはできません",
    ] {
        assert!(
            readme.contains(clause),
            "README is missing contract clause: {clause}"
        );
    }
}
