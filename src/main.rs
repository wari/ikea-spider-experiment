extern crate hyper;
extern crate kuchiki;
extern crate postgres;
extern crate url;
extern crate getopts;
extern crate undup;

// Std
use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::string::String;
use std::collections::BTreeMap;
use std::result;
use std::thread::sleep;
use std::time::{Duration,Instant};

// Hyper
use hyper::client::Client;
use hyper::client::response::Response;
use hyper::status::StatusCode;

// Kuchiki
use kuchiki::traits::*;
use kuchiki::NodeRef;
use kuchiki::NodeData::Element;

// Postgres
use postgres::{Connection, SslMode};

// URL
use url::percent_encoding::*;

// Getopts
use getopts::{Matches, Options};

// Undup
use undup::undup_chars;

type Result<T> = result::Result<T, hyper::error::Error>;

const BASE_ADDRESS: &'static str = "http://www.ikea.com";

struct Product {
    id: String,
    name: String,
    typ: String,
    country: String,
    unit: String,
    price: String,
    metric: String,
    image_url: String,
    url: String,
	department: String,
	category: String,
	subcategory: String,
	department_url: String,
	category_url: String,
	subcategory_url: String,
}

#[derive(Clone)]
struct Department {
    name: String,
    url: String,
}

struct Country<'a> {
    name: &'a str,
    url: &'a str,
}

enum Output {
    File(String),
    Database(Connection),
}

fn fetch_html(url: &str) -> Result<NodeRef> {
    let client = Client::new();
    let mut res = try!(client.get(url).send());

    let mut html = String::new();
    try!(res.read_to_string(&mut html));

    Ok(kuchiki::parse_html().one(html))
}

fn write_department_products(country: &Country, output: Output, mut error_str: &mut String) {
    let mut m = BTreeMap::<String, Product>::new();
    let mut visited_urls = BTreeMap::<String, bool>::new();

    let departments = match fetch_departments(&country) {
        Some(departments) => departments,
        None => return,
    };

    for department in departments {
        &fetch_products_from_all_departments(&mut visited_urls, &mut m, vec![department], &mut error_str);

        match output {
            Output::File(ref filename) => write_to_file(&m, &filename, country, &mut error_str),
            Output::Database(ref conn) => write_to_database(&m, &conn, country, &mut error_str),
        }
    }
}

fn write_to_file(m: &BTreeMap<String, Product>, output: &str, country: &Country, mut error_str: &mut String) {
    let max_count = m.len();
    let mut index = 1;

    let mut f = match File::create(output) {
        Ok(file) => file,
        Err(error) => panic!(error),
    };

    if let Err(error) = f.write_all(b"Item Number,Name,Type,Price,Unit,Metric,Image URL,URL,Department,Category,Subcategory,Department URL,Category URL, Subcategory URL\n") {
        panic!(error);
    }

    for i in m {
        if let Some(product) = fetch_product_info(i.0.as_str(), country, &mut error_str) {
            if let Err(error) = f.write_all(format!(
				     "\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"\n",
                     product.id,
                     product.name,
                     product.typ,
                     product.price,
                     product.unit,
                     product.metric,
                     product.image_url,
                     product.url,
                     &i.1.department,
                     &i.1.category,
                     &i.1.subcategory,
                     &i.1.department_url,
                     &i.1.category_url,
                     &i.1.subcategory_url,
				).as_bytes()) {

                panic!(error);

            }

            println!("{}: {}: {}: {} ({}/{})", &i.1.department, &i.1.category, &i.1.subcategory, product.name, index, max_count);
            index += 1;
        }
    }
}

fn write_to_database(m: &BTreeMap<String, Product>, conn: &Connection, country: &Country, mut error_str: &mut String) {
    let max_count = m.len();
    let mut index = 1;

    for i in m {
        if let Some(product) = fetch_product_info(i.0.as_str(), country, &mut error_str) {
            conn.execute("INSERT INTO product (
                              id,
                              name,
                              type,
                              country,
                              price,
                              unit,
                              metric,
                              url,
                              image_url,
                              department,
                              category,
                              subcategory,
                              department_url,
                              category_url,
                              subcategory_url,
                              created_at,
                              updated_at
                          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW(), NOW())
                            ON CONFLICT (id, country, url)
                            DO UPDATE SET
                                name=$2,
                                type=$3,
                                country=$4,
                                price=$5,
                                unit=$6,
                                metric=$7,
                                url=$8,
                                image_url=$9,
                                department=$10,
                                category=$11,
                                subcategory=$12,
                                department_url=$13,
                                category_url=$14,
                                subcategory_url=$15,
                                updated_at=NOW()",
                             &[
                                &product.id,
                                &product.name,
                                &product.typ,
                                &product.country,
                                &product.price,
                                &product.unit,
                                &product.metric,
                                &product.url,
                                &product.image_url,
                                &i.1.department,
                                &i.1.category,
                                &i.1.subcategory,
                                &i.1.department_url,
                                &i.1.category_url,
                                &i.1.subcategory_url,
                             ]).unwrap();
            println!("{}: {}: {}: {} ({}/{})", &i.1.department, &i.1.category, &i.1.subcategory, product.name, index, max_count);
            index += 1;
        }
    }
}

fn fetch_departments(country: &Country) -> Option<Vec<Department>> {
    let address = &format!("{}{}", BASE_ADDRESS, &country.url);
    let ref document = match fetch_html(address) {
        Ok(doc) => doc,
        Err(error) => {
            println!("error: fetch_departments: {:?}", error);
            return None;
        }
    };

    let mut departments = Vec::new();

    let matches = match document.select(".departmentLinkBlock a") {
        Ok(ms) => ms,
        Err(error) => {
            println!("error: fetch_departments: {:?}", error);
            return None;
        }
    };

    for css_match in matches {
        let node = css_match.as_node();

        let data_ref = match node.data().clone() {
            Element(data) => data.attributes.borrow().clone(),
            _ => continue,
        };

        let url = match data_ref.get("href") {
            None => continue,
            Some(url) => url.to_string(),
        };

        if url == "#" {
            continue;
        }

        let text_node = match node.first_child() {
            Some(text_node) => text_node,
            None => continue,
        };


        let name = match text_node.as_text() {
            Some(text) => text.borrow().trim().to_string(),
            None => continue,
        };

        departments.push(Department{
            url: url,
            name: name,
        });
    }

    return Some(departments);
}

fn fetch_products_from_all_departments(visited_urls: &mut BTreeMap<String, bool>, m: &mut BTreeMap<String, Product>, hierarchy: Vec<Department>, mut error_str: &mut String) {
    let department = if let Some(department) = hierarchy.last() {
        department
    } else {
        return;
    };

    let address = &format!("{}{}", BASE_ADDRESS, &department.url);
    let ref document = match fetch_html(address) {
        Ok(doc) => doc,
        Err(error) => {
            println!("error: fetch_products_from_all_departments: {:?}", error);
            error_str.push_str(&format!("Failed to fetch HTML at {}\n", address));
            return;
        }
    };

    if has_product(document) {
        let matches = match document.select("#productLists .productDetails a, .seoProduct") {
            Ok(ms) => ms,
            Err(error) => {
                println!("error: fetch_products_from_all_departments: {:?}", error);
                error_str.push_str(&format!("Failed to fetch product metadata at {}\n", address));
                return;
            }
        };

        for css_match in matches {
            let node = css_match.as_node();

            let data_ref = match node.data().clone() {
                Element(data) => data.attributes.borrow().clone(),
                _ => continue,
            };

            let url = match data_ref.get("href") {
                None => continue,
                Some(url) => url.to_string(),
            };

            if url == "#" {
                continue;
            }

            println!("PRODUCT URL {}", url);

            let product = Product{
                id: String::from(""),
                name: String::from(""),
                typ: String::from(""),
                country: String::from(""),
                price: String::from(""),
                unit: String::from(""),
                metric: String::from(""),
                image_url: String::from(""),
                url: url.clone(),
                department: hierarchy[0].name.clone(),
                category: if hierarchy.len() >= 2 { hierarchy[1].name.clone() } else { "".to_string() },
                subcategory: if hierarchy.len() >= 3 { hierarchy[2].name.clone() } else { "".to_string() },
                department_url: hierarchy[0].url.clone(),
                category_url: if hierarchy.len() >= 2 { hierarchy[1].url.clone() } else { "".to_string() },
                subcategory_url: if hierarchy.len() >= 3 { hierarchy[2].url.clone() } else { "".to_string() },
            };

            m.insert(url.clone(), product);
        }
    } else {
        let matches = match document.select(".visualNavContainer a") {
            Ok(ms) => ms,
            Err(error) => {
                println!("error: fetch_products_from_all_departments: {:?}", error);
                error_str.push_str(&format!("Failed to fetch department data at {}\n", address));
                return;
            }
        };

        for css_match in matches {
            let node = css_match.as_node();

            let data_ref = match node.data().clone() {
                Element(data) => data.attributes.borrow().clone(),
                _ => continue,
            };

            let mut department = Department {
                url: "".to_string(),
                name: "".to_string(),
            };
            department.url = match data_ref.get("href") {
                None => continue,
                Some(url) => url.to_string(),
            };


            let text = if let Some(text) = fetch_node_text(&node.parent().unwrap(), ".categoryContainer a:first-child") {
                text
            } else {
                continue;
            };

            department.name = text.clone();

            if visited_urls.contains_key(&department.url) {
                continue;
            }
            visited_urls.insert(department.url.clone(), true);

            let mut next_hierarchy = hierarchy.clone();
            next_hierarchy.push(department);

            fetch_products_from_all_departments(visited_urls, m, next_hierarchy, &mut error_str);
        }
    }
}

fn has_product(document: &NodeRef) -> bool {
    let matches = match document.select("#productLists .productDetails, .seoProduct") {
        Ok(ms) => ms,
        Err(_) => {
            return false;
        }
    };

    matches.count() > 0
}

fn fetch_product_info(url: &str, country: &Country, error_str: &mut String) -> Option<Product> {
    let address = format!("{}{}", BASE_ADDRESS, url);
    let document = match fetch_html(&address) {
        Ok(doc) => doc,
        Err(error) => {
            error_str.push_str(&format!("Failed to fetch product data at {}\n", &address));
            println!("error: fetch_product_info: {}", error);
            return None;
        }
    };

    Some(Product {
        id: fetch_node_text(&document, "#itemNumber").unwrap_or_default().replace(".", ""),
        name: undup_chars(&fetch_node_text(&document, "#name").unwrap_or_default(), vec![' ']).replace("\n", ""),
        typ: fetch_node_text(&document, "#type").unwrap_or_default(),
        price: fetch_node_text(&document, "#price1").unwrap_or_default(),
        country: country.name.to_string(),
        unit: fetch_node_text(&document, ".productunit").unwrap_or_default(),
        metric: fetch_node_text(&document, "#metric").unwrap_or_default(),
        image_url: fetch_node_attr_value(&document, "#productImg", "src").unwrap_or_default(),
        url: String::from(url),
		department: "".to_string(),
		category: "".to_string(),
		subcategory: "".to_string(),
		department_url: "".to_string(),
		category_url: "".to_string(),
		subcategory_url: "".to_string(),
    })
}

fn fetch_node_text(document: &NodeRef, css_selector: &str) -> Option<String> {
    let css_matches = match document.select(css_selector) {
        Ok(css_matches) => css_matches,
        Err(_) => return None,
    };

    let css_match = match css_matches.last() {
        Some(css_match) => css_match,
        None => return None,
    };

    let text_node = match css_match.as_node().first_child() {
        Some(text_node) => text_node,
        None => return None,
    };

    match text_node.as_text() {
        Some(text) => Some(text.borrow().trim().to_string()),
        None => None,
    }
}

fn fetch_node_attr_value(document: &NodeRef, css_selector: &str, name: &str) -> Option<String> {
    let css_matches = match document.select(css_selector) {
        Ok(css_matches) => css_matches,
        Err(_) => return None,
    };

    let css_match = match css_matches.last() {
        Some(css_match) => css_match,
        None => return None,
    };

    let data = match css_match.as_node().data().clone() {
        Element(data) => data,
        _ => return None,
    };

    let attributes = data.attributes.borrow();
    match attributes.get(name) {
        Some(value) => Some(value.to_string()),
        None => None,
    }
}

fn do_file(country: &Country, matches: &Matches) {
    let output = match matches.opt_str("o") {
        Some(o) => o,
        None => "output.csv".to_string(),
    };

    let mut error_str = String::new();
    write_department_products(country, Output::File(output), &mut error_str);
}

fn do_database(country: &Country, matches: &Matches) -> String {
    let dbhost: String = match matches.opt_str("dbhost") {
        Some(t) => t,
        None => "localhost".to_string(),
    };

    let dbport: String = match matches.opt_str("dbport") {
        Some(t) => t,
        None => "5432".to_string(),
    };

    let dbuser: String = match matches.opt_str("dbuser") {
        Some(t) => percent_encode(t.as_bytes(), USERINFO_ENCODE_SET).collect::<String>(),
        None => "postgres".to_string(),
    };

    let dbpass: String = match matches.opt_str("dbpass") {
        Some(t) => format!(":{}", percent_encode(t.as_bytes(), USERINFO_ENCODE_SET)),
        None => "".to_string(),
    };

    let conn = Connection::connect(format!("postgres://{}{}@{}:{}", dbuser, dbpass, dbhost, dbport).as_str(), SslMode::None).unwrap();
    let _ = conn.execute(
        "CREATE TABLE product (
                     id              VARCHAR NOT NULL,
                     name            VARCHAR NOT NULL,
                     type            VARCHAR NOT NULL,
                     country         VARCHAR NOT NULL,
                     price           VARCHAR NOT NULL,
                     unit            VARCHAR NOT NULL,
                     metric          VARCHAR NOT NULL,
                     url             VARCHAR NOT NULL,
                     image_url       VARCHAR NOT NULL,
                     department      VARCHAR NOT NULL,
                     category        VARCHAR NOT NULL,
                     subcategory     VARCHAR NOT NULL,
                     department_url  VARCHAR NOT NULL,
                     category_url    VARCHAR NOT NULL,
                     subcategory_url VARCHAR NOT NULL,
                     created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
                     updated_at      TIMESTAMP WITH TIME ZONE NOT NULL,
                     UNIQUE (id, country, url)
         )", &[]);

    let mut error_str = String::new();
    write_department_products(country, Output::Database(conn), &mut error_str);
    error_str
}

fn report_error(error_str: &str, emails: &Vec<String>) -> Result<Response> {
    let client = Client::new();

    // Format emails into query format
    let mut formatted_emails = String::new();
    for email in emails {
        formatted_emails.push_str(&format!("&to={}", &email));
    }

    let message = percent_encode(format!("http://email.bbh-labs.com.sg?from=BBH Labs <postmaster@mail.bbh-labs.com.sg>&subject=Error: IKEA Spider&text={}{}", error_str, formatted_emails).as_bytes(), QUERY_ENCODE_SET).collect::<String>();
    let res = try!(client.post(&message).send());
    Ok(res)
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn print_countries(countries: &[Country]) {
    let mut i = 0;

    println!("Select a country index from the following list (specify using -c flag):");
    for country in countries {
        println!("{}: {}", i, &country.name);
        i += 1;
    }
}

fn main() {
    let countries = [
        Country { name: "Singapore", url: "/sg/en" },
        Country { name: "Malaysia English", url: "/my/en" },
        Country { name: "Malaysia Bahasa", url: "/my/ms" },
        Country { name: "Thailand Thai", url: "/th/th" },
        Country { name: "Thailand English", url: "/th/en" },
    ];

    // Parse program arguments
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("t",
                "type",
                "set type of backend",
                "TYPE");
    opts.optopt("o",
                "output",
                "set output file name",
                "FILE");
    opts.optopt("c",
                "country",
                "set country index",
                "COUNTRY INDEX");
    opts.optopt("",
                "dbhost",
                "set database host",
                "DBHOST");
    opts.optopt("",
                "dbport",
                "set database port",
                "DBPORT");
    opts.optopt("",
                "dbuser",
                "set database username",
                "DBUSER");
    opts.optopt("",
                "dbpass",
                "set database password",
                "DBPASS");
    opts.optopt("i",
                "interval",
                "set loop interval in seconds (default: 60)",
                "SECS");
    opts.optmulti("e", "email", "email to this address if there's an error", "EMAIL");
    opts.optflag("l", "loop", "forever scrape the website");
    opts.optflag("h", "help", "print this help menu");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let country = match matches.opt_str("c") {
        Some(index_str) => {
            if let Ok(index) = index_str.parse::<usize>() {
                if index > countries.len() - 1{
                    println!("Country index is too big!");
                    return;
                }
                &countries[index]
            } else {
                println!("Invalid country index!");
                return;
            }
        },
        None => {
            print_countries(&countries);
            return;
        },
    };

    let typ: String = match matches.opt_str("t") {
        Some(t) => t,
        None => "file".to_string(),
    };

    let interval = match matches.opt_str("i") {
        Some(t) => match t.parse::<u64>() {
            Ok(secs) => secs,
            Err(_) => {
                println!("Argument passed to -i or --interval is not a number!");
                return;
            },
        },
        None => 60,
    };

    let emails = matches.opt_strs("e");
    {
        if emails.len() > 0 {
            print!("Will email errors to: ");
            for email in &emails {
                print!("{} ", &email);
            }
            println!("");
        }
    }

    loop {
        let start_time = Instant::now();

        if typ == "file" {
            do_file(country, &matches);
        } else if typ == "database" {
            let error_str = &do_database(country, &matches);
            if error_str.len() > 0 {
                match report_error(error_str, &emails) {
                    Ok(res) => if res.status == StatusCode::Ok {
                        println!("Successfully reported error");
                    } else {
                        println!("Failed to report error: {}", res.status);
                    },
                    Err(err) => {
                        println!("Failed to report error: {}", err);
                    },
                }
            }
        }

        if !matches.opt_present("loop") {
            break;
        }

        let elapsed_duration = start_time.elapsed();
        let interval_duration = Duration::new(interval, 0);
        if interval_duration > elapsed_duration {
            sleep(interval_duration - elapsed_duration);
        }
    }
}
