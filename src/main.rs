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
use std::collections::HashMap;
use std::result;

// Hyper
use hyper::client::Client;

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

fn get_html(url: &str) -> Result<NodeRef> {
    let client = Client::new();
    let mut res = try!(client.get(url).send());

    let mut html = String::new();
    try!(res.read_to_string(&mut html));

    Ok(kuchiki::parse_html().one(html))
}

fn write_department_products(country: &Country, output: Output) {
    let mut hashmap = HashMap::<String, Product>::new();
    let mut visited_urls = HashMap::<String, bool>::new();

    let department = Department {
        url: format!("{}/catalog/categories/departments/childrens_ikea/", &country.url),
        name: String::from("Children's IKEA"),
    };

    println!("{}", &department.url);

    get_products_from_all_departments(&mut visited_urls, &mut hashmap, vec![department]);
    println!("Total products: {}\n", hashmap.len());

    match output {
        Output::File(filename) => write_to_file(hashmap, &filename, country),
        Output::Database(conn) => write_to_database(hashmap, &conn, country),
    }
}

fn write_to_file(hashmap: HashMap<String, Product>, output: &str, country: &Country) {
    let max_count = hashmap.len();
    let mut index = 1;

    let mut f = match File::create(output) {
        Ok(file) => file,
        Err(error) => panic!(error),
    };

    if let Err(error) = f.write_all(b"Item Number,Name,Type,Price,Unit,Metric,Image URL,URL,Department,Category,Subcategory,Department URL,Category URL, Subcategory URL\n") {
        panic!(error);
    }

    for i in hashmap {
        if let Some(product) = get_product_info(i.0.as_str(), country) {
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

            println!("{}/{}: {}", index, max_count, product.name);
            index += 1;
        }
    }
}

fn write_to_database(hashmap: HashMap<String, Product>, conn: &Connection, country: &Country) {
    let max_count = hashmap.len();
    let mut index = 1;

    for i in hashmap {
        if let Some(product) = get_product_info(i.0.as_str(), country) {
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
            println!("{}/{}: {}", index, max_count, product.name);
            index += 1;
        }
    }
}

fn get_products_from_all_departments(visited_urls: &mut HashMap<String, bool>, hashmap: &mut HashMap<String, Product>, hierarchy: Vec<Department>) {
    let department = if let Some(department) = hierarchy.last() {
        department
    } else {
        return;
    };

    let address = &format!("{}{}", BASE_ADDRESS, &department.url);
    let ref document = match get_html(address) {
        Ok(doc) => doc,
        Err(error) => {
            println!("error: get_products_from_all_departments: {:?}", error);
            return;
        }
    };

    if has_product(document) {
        let matches = match document.select("#productLists .productDetails a, .seoProduct") {
            Ok(ms) => ms,
            Err(error) => {
                println!("error: get_products_from_all_departments: {:?}", error);
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

            hashmap.insert(url.clone(), product);
        }
    } else {
        let matches = match document.select(".visualNavContainer a") {
            Ok(ms) => ms,
            Err(error) => {
                println!("error: get_products_from_all_departments: {:?}", error);
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


            let text = if let Some(text) = get_node_text(&node.parent().unwrap(), ".categoryContainer a:first-child") {
                text
            } else {
                continue;
            };

            department.name = text.clone();

            if visited_urls.contains_key(&department.url) {
                continue;
            }
            visited_urls.insert(department.url.clone(), true);

            println!("DEPARTMENT {}", &department.name);

            let mut next_hierarchy = hierarchy.clone();
            next_hierarchy.push(department);

            get_products_from_all_departments(visited_urls, hashmap, next_hierarchy);
        }
    }
}

fn has_product(document: &NodeRef) -> bool {
    let matches = match document.select("#productLists .productDetails, .seoProduct") {
        Ok(ms) => ms,
        Err(error) => {
            println!("error: has_product: {:?}", error);
            return false;
        }
    };

    matches.count() > 0
}

fn get_product_info(url: &str, country: &Country) -> Option<Product> {
    let document = match get_html(format!("{}{}", BASE_ADDRESS, url).as_str()) {
        Ok(doc) => doc,
        Err(error) => {
            println!("error: get_product_info: {}", error);
            return None;
        }
    };

    Some(Product {
        id: get_node_text(&document, "#itemNumber").unwrap_or_default(),
        name: undup_chars(&get_node_text(&document, "#name").unwrap_or_default(), vec![' ']).replace("\n", ""),
        typ: get_node_text(&document, "#type").unwrap_or_default(),
        price: get_node_text(&document, "#price1").unwrap_or_default(),
        country: country.name.to_string(),
        unit: get_node_text(&document, ".productunit").unwrap_or_default(),
        metric: get_node_text(&document, "#metric").unwrap_or_default(),
        image_url: get_node_attr_value(&document, "#productImg", "src").unwrap_or_default(),
        url: String::from(url),
		department: "".to_string(),
		category: "".to_string(),
		subcategory: "".to_string(),
		department_url: "".to_string(),
		category_url: "".to_string(),
		subcategory_url: "".to_string(),
    })
}

fn get_node_text(document: &NodeRef, css_selector: &str) -> Option<String> {
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

fn get_node_attr_value(document: &NodeRef, css_selector: &str, name: &str) -> Option<String> {
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

    write_department_products(country, Output::File(output));
}

fn do_database(country: &Country, matches: &Matches) {
    let dbhost: String = match matches.opt_str("dbhost") {
        Some(t) => t,
        None => "localhost".to_string(),
    };

    let dbport: String = match matches.opt_str("dbport") {
        Some(t) => t,
        None => "5432".to_string(),
    };

    let dbuser: String = match matches.opt_str("dbuser") {
        Some(t) => percent_encode(t.as_bytes(), FORM_URLENCODED_ENCODE_SET),
        None => "postgres".to_string(),
    };

    let dbpass: String = match matches.opt_str("dbpass") {
        Some(t) => format!(":{}", percent_encode(t.as_bytes(), FORM_URLENCODED_ENCODE_SET)),
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

    write_department_products(country, Output::Database(conn));
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

    loop {
        if typ == "file" {
            do_file(country, &matches);
        } else if typ == "database" {
            do_database(country, &matches);
        }

        if !matches.opt_present("loop") {
            break;
        }
    }
}
