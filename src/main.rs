extern crate hyper;
extern crate kuchiki;
extern crate getopts;

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

// Getopts
use getopts::{Matches, Options};

type Result<T> = result::Result<T, hyper::error::Error>;

const BASE_ADDRESS: &'static str = "http://www.ikea.com";

struct Product {
    item_number: String,
    name: String,
    typ: String,
    unit: String,
    price: String,
    metric: String,
    image_url: String,
    url: String,
	department: Option<Department>,
	category: Option<Department>,
	subcategory: Option<Department>,
}

#[derive(Clone)]
struct Department {
    name: String,
    url: String,
}

fn get_html(url: &str) -> Result<NodeRef> {
    let client = Client::new();
    let mut res = try!(client.get(url).send());

    let mut html = String::new();
    try!(res.read_to_string(&mut html));

    Ok(kuchiki::parse_html().one(html))
}

fn write_department_products_to_file(output: &str) {
    let mut f = match File::create(output) {
        Ok(file) => file,
        Err(error) => panic!(error),
    };

    let mut ps = HashMap::<String, Product>::new();
    let mut visited_urls = HashMap::<String, bool>::new();

    /*
    let ref address = format!("{}/sg/en", BASE_ADDRESS);
    let ref document = match get_html(address) {
        Ok(doc) => doc,
        Err(error) => {
            println!("error: write_department_products_to_file: {:?}", error);
            return;
        }
    };

    let matches = match document.select(".departmentLinkBlock a") {
        Ok(ms) => ms,
        Err(error) => {
            println!("error: get_departments_or_products: {:?}", error);
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

        if visited_urls.contains_key(&department.url) {
            continue;
        }
        visited_urls.insert(department.url.clone(), true);

        let text_node = match node.first_child() {
            Some(text_node) => text_node,
            None => continue,
        };

        department.name = match text_node.as_text() {
            Some(text) => text.borrow().trim().to_string(),
            None => continue,
        };

        get_product_urls_from_all_departments(&mut visited_urls, ps, vec![department]);
    }
    */

    let department = Department {
        url: String::from("/sg/en/catalog/categories/departments/childrens_ikea/"),
        name: String::from("Children's IKEA"),
    };

    get_products_from_all_departments(&mut visited_urls, &mut ps, vec![department]);

    println!("Total products: {}\n", ps.len());

    if let Err(error) = f.write_all(b"Item Number,Name,Type,Price,Unit,Metric,Image URL,URL,Department,Category,Subcategory,Department URL,Category URL, Subcategory URL\n") {
        panic!(error);
    }

    let max_count = ps.len();
    let mut index = 1;
    for i in ps {
        if let Some(product) = get_product_info(i.0.as_str()) {
            let department = i.1.department;
            let category = i.1.category;
            let subcategory = i.1.subcategory;
            let empty_string = &String::from("");
            if let Err(error) = f.write_all(format!(
				     "\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"\n",
                     product.item_number,
                     product.name,
                     product.typ,
                     product.price,
                     product.unit,
                     product.metric,
                     product.image_url,
                     product.url,
                     if let Some(ref d) = department { &d.name } else { empty_string },
                     if let Some(ref c) = category { &c.name } else { empty_string },
                     if let Some(ref sc) = subcategory { &sc.name } else { empty_string },
                     if let Some(ref d) = department { &d.url } else { empty_string },
                     if let Some(ref c) = category { &c.url } else { empty_string },
                     if let Some(ref sc) = subcategory { &sc.url } else { empty_string },
				).as_bytes()) {

                panic!(error);

            }

            println!("{}/{}: {}", index, max_count, product.name);
            index += 1;
        }
    }
}

fn get_products_from_all_departments(visited_urls: &mut HashMap<String, bool>, ps: &mut HashMap<String, Product>, hierarchy: Vec<Department>) {
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

            ps.insert(url.clone(), Product{
                item_number: String::from(""),
                name: String::from(""),
                typ: String::from(""),
                price: String::from(""),
                unit: String::from(""),
                metric: String::from(""),
                image_url: String::from(""),
                url: url.clone(),
				department: Some(hierarchy[0].clone()),
				category: if hierarchy.len() >= 2 { Some(hierarchy[1].clone()) } else { None },
				subcategory: if hierarchy.len() >= 3 { Some(hierarchy[2].clone()) } else { None },
            });
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

            get_products_from_all_departments(visited_urls, ps, next_hierarchy);
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

fn get_product_info(url: &str) -> Option<Product> {
    let document = match get_html(format!("{}{}", BASE_ADDRESS, url).as_str()) {
        Ok(doc) => doc,
        Err(error) => {
            println!("error: get_product_info: {}", error);
            return None;
        }
    };

    Some(Product {
        item_number: get_node_text(&document, "#itemNumber").unwrap_or_default(),
        name: get_node_text(&document, "#name").unwrap_or_default(),
        typ: get_node_text(&document, "#type").unwrap_or_default(),
        price: get_node_text(&document, "#price1").unwrap_or_default(),
        unit: get_node_text(&document, ".productunit").unwrap_or_default(),
        metric: get_node_text(&document, "#metric").unwrap_or_default(),
        image_url: get_node_attr_value(&document, "#productImg", "src").unwrap_or_default(),
        url: String::from(url),
		department: None,
		category: None,
		subcategory: None,
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

fn do_file(matches: &Matches) {
    let output = match matches.opt_str("o") {
        Some(o) => o,
        None => "output.csv".to_string(),
    };

    write_department_products_to_file(&output);
    // write_all_products_to_file(&output);
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn main() {
    // Parse program arguments
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("t",
                "type",
                "set type of data to be served which can be file or http (default: file)",
                "TYPE");
    opts.optopt("o",
                "output",
                "set output file name if served as file (default: output.csv)",
                "FILE");
    opts.optflag("h", "help", "print this help menu");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let typ: String = match matches.opt_str("t") {
        Some(t) => t,
        None => "file".to_string(),
    };

    if typ == "file" {
        do_file(&matches);
    } else {

    }
}
