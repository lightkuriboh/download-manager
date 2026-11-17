use super::*;

#[tokio::test]
async fn frames_are_little_endian_and_support_split_reads() {
    let (mut reader, mut writer) = tokio::io::duplex(2);
    let sender = async {
        write_frame(&mut writer, &"example").await.unwrap();
    };
    let receiver = async {
        let mut length = [0; 4];
        reader.read_exact(&mut length).await.unwrap();
        assert_eq!(length, [9, 0, 0, 0]);
        let mut body = [0; 9];
        reader.read_exact(&mut body).await.unwrap();
        assert_eq!(&body, br#""example""#);
    };
    tokio::join!(sender, receiver);
}

#[tokio::test]
async fn invalid_lengths_and_truncated_frames_fail() {
    for length in [0, MAX_FRAME_BYTES as u32 + 1] {
        assert!(matches!(
            read_frame::<Request>(&mut length.to_le_bytes().as_slice()).await,
            Err(Error::FrameLength)
        ));
    }
    assert!(matches!(
        read_frame::<Request>(&mut b"\x04\x00\x00\x00{}".as_slice()).await,
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof
    ));
}

#[test]
fn command_payload_rejects_unknown_and_duplicate_fields() {
    for payload in [
        r#"{"job_id":"2","other":true}"#,
        r#"{"job_id":"2","job_id":"3"}"#,
    ] {
        let json = format!(
            r#"{{"protocol":1,"request_id":"{}","command":"job.retry","payload":{payload}}}"#,
            Uuid::nil(),
        );
        let request: Request = serde_json::from_str(&json).unwrap();
        assert!(Command::decode(&request).is_err());
    }
    let id = JobId::new(i64::MAX).unwrap();
    assert_eq!(
        serde_json::from_str::<JobId>(&serde_json::to_string(&id).unwrap()).unwrap(),
        id
    );
}
