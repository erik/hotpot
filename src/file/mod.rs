use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

use anyhow::{Result, anyhow};
use flate2::read::GzDecoder;

use crate::activity::RawActivity;

pub mod fit;
pub mod gpx;
pub mod tcx;

#[derive(Debug)]
pub enum MediaType {
    Gpx,
    Fit,
    Tcx,
}

#[derive(Debug)]
pub enum Compression {
    None,
    Gzip,
}

pub fn read<R>(rdr: R, kind: MediaType, comp: Compression) -> Result<Option<RawActivity>>
where
    R: Read + 'static,
{
    let mut reader: BufReader<Box<dyn Read>> = BufReader::new(match comp {
        Compression::None => Box::new(rdr),
        Compression::Gzip => Box::new(GzDecoder::new(rdr)),
    });

    match kind {
        MediaType::Gpx => gpx::parse_gpx(&mut reader),
        MediaType::Tcx => tcx::parse_tcx(&mut reader),
        MediaType::Fit => fit::parse_fit(&mut reader),
    }
}

pub fn read_file(p: &Path) -> Result<Option<RawActivity>> {
    let Some(file_name) = p.file_name().and_then(|f| f.to_str()) else {
        return Err(anyhow!("no file name"));
    };

    let Some((media_type, comp)) = get_file_type(file_name) else {
        // Just skip over unsupported file types.
        return Ok(None);
    };

    let file = File::open(p)?;
    read(file, media_type, comp)
}

pub fn get_file_type(file_name: &str) -> Option<(MediaType, Compression)> {
    let mut exts = file_name.rsplit('.');

    let (comp, ext) = match exts.next()? {
        "gz" => (Compression::Gzip, exts.next()?),
        ext => (Compression::None, ext),
    };

    match ext {
        "gpx" => Some((MediaType::Gpx, comp)),
        "fit" => Some((MediaType::Fit, comp)),
        "tcx" => Some((MediaType::Tcx, comp)),
        _ => None,
    }
}
